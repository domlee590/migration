# Quick Start: Lean Weaviate → Qdrant Migration

## ✅ What Was Modified

Three files were changed to add property filtering:

1. **`pkg/commons/config.go`**: Added `IncludeProperties []string` to `WeaviateConfig`
2. **`cmd/migrate_from_weaviate.go`**: Added filtering logic (lines ~240-280 and ~350-360)
3. **Build scripts**: `build-local.sh`, `build-docker.sh`

## 🚀 Build & Run

### Step 1: Build

```bash
cd /Users/vecflow/Workspace2/migration
./build-local.sh
```

Binary will be at: `./bin/qdrant-migration`

### Step 2: Create Qdrant Collection

**IMPORTANT**: Must create collection BEFORE migration (tool requires it to exist).

```python
from qdrant_client import QdrantClient
from qdrant_client.models import *

client = QdrantClient(
    url="https://49e2f807-f34d-481f-863a-3027135c93c2.us-east-1-1.aws.cloud.qdrant.io",
    api_key="your-key"
)

client.create_collection(
    collection_name="chunks",
    vectors_config=VectorParams(
        size=1024,
        distance=Distance.COSINE,
        on_disk=True,
    ),
    shard_number=6,  # USE ALL 3 NODES (3 × 2 = 6 shards)
    replication_factor=2,
    on_disk_payload=True,
    quantization_config=ScalarQuantization(
        scalar=ScalarQuantizationConfig(
            type=ScalarType.INT8,
            quantile=0.99,
            always_ram=False,
        ),
    ),
)

# Create indexes
client.create_payload_index("chunks", "doc_id", PayloadSchemaType.KEYWORD)
client.create_payload_index("chunks", "data_source_id", PayloadSchemaType.KEYWORD)
```

### Step 3: Run Lean Migration

```bash
./bin/qdrant-migration weaviate \
  --weaviate.host 'your-cluster.weaviate.cloud' \
  --weaviate.scheme 'https' \
  --weaviate.auth-type 'apiKey' \
  --weaviate.api-key 'YOUR_WEAVIATE_KEY' \
  --weaviate.class-name 'Text_tables' \
  --weaviate.include-properties 'doc_id,data_source_id' \
  --qdrant.url 'https://49e2f807-f34d-481f-863a-3027135c93c2.us-east-1-1.aws.cloud.qdrant.io:6334' \
  --qdrant.api-key 'YOUR_QDRANT_KEY' \
  --qdrant.collection 'chunks' \
  --migration.batch-size 100
```

**Key flag**: `--weaviate.include-properties 'doc_id,data_source_id'` 

This filters to only these 2 fields (+ vectors). Omit this flag to migrate all properties.

## 📊 What You Get

### Before (All Properties)
```python
{
    "id": "uuid",
    "vector": [...],  # 1024 floats
    "payload": {
        "doc_id": "...",
        "data_source_id": "...",
        "page_content": "Full text...",  # 500-2000 bytes
        "chunk_index": 42,
        "page_number": 5,
        # ... 20+ other fields
    }
}
```
**Size:** ~2-3 KB/point  
**Disk (350M points):** ~1.5-2 TB per replica

### After (Lean)
```python
{
    "id": "uuid",  # chunk_id - use for DynamoDB lookup
    "vector": [...],  # 1024 floats
    "payload": {
        "doc_id": "...",
        "data_source_id": "..."
    }
}
```
**Size:** ~1.1 KB/point  
**Disk (350M points):** ~400-500 GB per replica

## 🔧 Update RAG Code

After migration, update your RAG retrieval in `rossAI-tabular-review`:

**File:** `src/processing/rag_retrieval.py` (lines ~400-416)

**Before:**
```python
for scored_point in fused_list:
    payload = scored_point.payload
    page_content = payload.get("page_content", "")  # ❌ Not in Qdrant anymore
    metadata = {k: v for k, v in payload.items() if k != "page_content"}
    docs.append(Document(page_content=page_content, metadata=metadata))
```

**After:**
```python
# 1. Get chunk_ids from Qdrant point IDs
chunk_ids = [str(sp.id) for sp in fused_list]

# 2. Batch fetch from DynamoDB
from src.clients.dynamodb_chunks import get_chunks_client
chunks_client = get_chunks_client()
chunks_map = chunks_client.get_chunks_by_ids(chunk_ids)

# 3. Build documents
for scored_point in fused_list:
    chunk_id = str(scored_point.id)
    chunk_data = chunks_map.get(chunk_id, {})
    page_content = chunk_data.get("page_content", "")
    
    # Metadata from Qdrant payload + DynamoDB
    metadata = dict(scored_point.payload)
    metadata["chunk_id"] = chunk_id
    if chunk_data:
        metadata["chunk_index"] = chunk_data.get("chunk_index")
        metadata["page_number"] = chunk_data.get("page_number")
    
    docs.append(Document(page_content=page_content, metadata=metadata))
```

**Latency:** +5-15ms for DynamoDB batch get (negligible vs LLM call)

## 📈 Expected Throughput

**With your 3-node cluster:**
- Batch size 100: ~20-40K points/sec
- Batch size 200: ~30-60K points/sec (if stable)

**For 350M points:**
- Low estimate: ~2.4 hours (40K/sec)
- High estimate: ~4.9 hours (20K/sec)

**Much faster than rossAI-migrator** because:
- Direct Weaviate → Qdrant (no S3 intermediate)
- No parquet serialization overhead
- gRPC batching

## 🎯 Qdrant Cluster Configuration

Your current setup has an issue:
- **Current:** 1 shard, RF=2 → only 2 of 3 nodes used
- **Recommended:** 6 shards, RF=2 → all 3 nodes used

When creating the collection, use `shard_number=6` to distribute across all nodes.

## 🆘 Troubleshooting

**"target collection does not exist"**
→ Create collection first (see Step 2)

**"property X not found"**
→ Check property name matches Weaviate schema exactly

**Out of memory**
→ Reduce `--migration.batch-size` to 50 or 25

**Tool crashes/restarts**
→ Migration is resumable! Just re-run the same command. It uses `_migration_offsets` collection to track progress.

## 📚 Full Documentation

See `LEAN_PAYLOAD_GUIDE.md` for complete details on:
- Storage calculations
- RAG patterns
- Performance tuning
- Docker deployment

## 🔄 Migration Status

Monitor in real-time:

```bash
# Watch Qdrant point count
watch -n 5 'curl -s "https://49e2f807-f34d-481f-863a-3027135c93c2.us-east-1-1.aws.cloud.qdrant.io/collections/chunks" \
  -H "api-key: YOUR_KEY" | jq .result.points_count'
```

Target: 350,000,000 points
