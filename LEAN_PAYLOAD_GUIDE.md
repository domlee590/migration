# Lean Payload Migration Guide

This fork adds **property filtering** support to the Weaviate → Qdrant migration, allowing you to migrate only specific fields to reduce Qdrant storage and memory usage.

## What Changed

### Added Configuration
- **`--weaviate.include-properties`**: Comma-separated list of properties to include in Qdrant payload
  - If empty/omitted: All properties are migrated (default behavior)
  - If specified: Only listed properties are migrated

### Modified Files
1. **`pkg/commons/config.go`**: Added `IncludeProperties []string` field to `WeaviateConfig`
2. **`cmd/migrate_from_weaviate.go`**: Added filtering logic for properties during migration

## Use Case: Lean RAG Storage

For large-scale RAG systems (300M+ chunks), storing full text in the vector DB wastes disk/RAM. The recommended pattern is:

**Store in Qdrant (lean):**
- Vectors (embeddings)
- Filter fields: `doc_id`, `data_source_id`
- Point ID = `chunk_id` (for DynamoDB lookup)

**Store in DynamoDB/Supabase (full):**
- Full `page_content` text
- All metadata

**Result:** 
- ~5x disk savings in Qdrant (~400-500 GB vs ~2 TB for 350M points)
- Much more headroom for growth
- RAG queries: vector search → fetch text by chunk_id from DB

## Building

### Option 1: Local Binary (Faster)

```bash
cd /Users/vecflow/Workspace2/migration
./build-local.sh
```

Binary will be at: `./bin/qdrant-migration`

### Option 2: Docker Image (Portable)

```bash
cd /Users/vecflow/Workspace2/migration
./build-docker.sh
```

Image will be tagged as: `qdrant-migration-lean:latest`

## Usage

### Lean Migration (Recommended for RAG)

Only migrate `doc_id` and `data_source_id` to Qdrant:

```bash
# Using local binary
./bin/qdrant-migration weaviate \
  --weaviate.host 'your-cluster.weaviate.cloud' \
  --weaviate.scheme 'https' \
  --weaviate.auth-type 'apiKey' \
  --weaviate.api-key 'your-weaviate-key' \
  --weaviate.class-name 'Text_tables' \
  --weaviate.include-properties 'doc_id,data_source_id' \
  --qdrant.url 'https://your-cluster.aws.cloud.qdrant.io:6334' \
  --qdrant.api-key 'your-qdrant-key' \
  --qdrant.collection 'chunks' \
  --migration.batch-size 100

# Using Docker
docker run --rm -it qdrant-migration-lean:latest weaviate \
  --weaviate.host 'your-cluster.weaviate.cloud' \
  --weaviate.scheme 'https' \
  --weaviate.auth-type 'apiKey' \
  --weaviate.api-key 'your-weaviate-key' \
  --weaviate.class-name 'Text_tables' \
  --weaviate.include-properties 'doc_id,data_source_id' \
  --qdrant.url 'https://your-cluster.aws.cloud.qdrant.io:6334' \
  --qdrant.api-key 'your-qdrant-key' \
  --qdrant.collection 'chunks' \
  --migration.batch-size 100
```

### Full Migration (Default Behavior)

Migrate all properties (same as original tool):

```bash
./bin/qdrant-migration weaviate \
  --weaviate.host 'your-cluster.weaviate.cloud' \
  --weaviate.scheme 'https' \
  --weaviate.auth-type 'apiKey' \
  --weaviate.api-key 'your-weaviate-key' \
  --weaviate.class-name 'Text_tables' \
  --qdrant.url 'https://your-cluster.aws.cloud.qdrant.io:6334' \
  --qdrant.api-key 'your-qdrant-key' \
  --qdrant.collection 'chunks' \
  --migration.batch-size 100
```

(Note: No `--weaviate.include-properties` flag = all properties)

## Qdrant Collection Setup

**Before migration**, create your Qdrant collection with the proper configuration:

```python
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    VectorParams,
    ScalarQuantization,
    ScalarQuantizationConfig,
    ScalarType,
    PayloadSchemaType,
)

client = QdrantClient(
    url="https://your-cluster.aws.cloud.qdrant.io",
    api_key="your-key"
)

# Create collection with quantization and proper settings
client.create_collection(
    collection_name="chunks",
    vectors_config=VectorParams(
        size=1024,  # voyage-3 embeddings
        distance=Distance.COSINE,
        on_disk=True,  # Save RAM
    ),
    shard_number=6,  # Use all nodes (3 nodes × 2)
    replication_factor=2,  # HA
    on_disk_payload=True,  # Save RAM
    quantization_config=ScalarQuantization(
        scalar=ScalarQuantizationConfig(
            type=ScalarType.INT8,
            quantile=0.99,
            always_ram=False,
        ),
    ),
)

# Create payload indexes for filtering
client.create_payload_index(
    collection_name="chunks",
    field_name="doc_id",
    field_schema=PayloadSchemaType.KEYWORD,
)

client.create_payload_index(
    collection_name="chunks",
    field_name="data_source_id",
    field_schema=PayloadSchemaType.KEYWORD,
)
```

## Resulting Qdrant Point Structure

### Lean (with `--weaviate.include-properties 'doc_id,data_source_id'`)

```python
{
    "id": "550e8400-e29b-41d4-a716-446655440000",  # chunk_id (use for DynamoDB lookup)
    "vector": [0.0123, -0.0456, ...],  # 1024 floats
    "payload": {
        "doc_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
        "data_source_id": "a1b2c3d4-5678-90ab-cdef-1234567890ab"
    }
}
```

**Size:** ~1.1 KB per point (quantized)

### Full (no filtering)

```python
{
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "vector": [0.0123, -0.0456, ...],
    "payload": {
        "doc_id": "...",
        "data_source_id": "...",
        "page_content": "Full text here...",  # 500-2000 bytes
        "chunk_index": 42,
        "page_number": 5,
        # ... all other fields
    }
}
```

**Size:** ~2-3 KB per point

## RAG Retrieval Pattern

After lean migration, update your RAG retrieval:

```python
from clients.dynamodb_chunks import get_chunks_client

chunks_client = get_chunks_client()

# 1. Vector search in Qdrant (returns lean payloads)
results = qdrant_client.search(
    collection_name="chunks",
    query_vector=embedding,
    query_filter={"doc_id": doc_id},
    limit=10,
)

# 2. Extract chunk IDs from point IDs
chunk_ids = [str(result.id) for result in results]

# 3. Fetch full text from DynamoDB
chunks_map = chunks_client.get_chunks_by_ids(chunk_ids)

# 4. Build documents with full text
for result in results:
    chunk_id = str(result.id)
    chunk_data = chunks_map.get(chunk_id, {})
    page_content = chunk_data.get("page_content", "")
    # Use page_content for LLM context
```

**Latency impact:** +5-15ms for DynamoDB batch get (negligible vs LLM call)

## Storage Comparison (350M Points)

| Configuration | Disk per Replica | Total (RF=2) |
|---------------|------------------|--------------|
| **Lean** (IDs only) | ~400-500 GB | ~800 GB-1 TB |
| **Full** (with page_content) | ~1.5-2 TB | ~3-4 TB |

**With 3 nodes × 2TB disks = 6TB raw capacity:**
- Lean: Comfortable (13-25% utilization)
- Full: Tight (50-67% utilization, risky during ingestion)

## Performance Tips

1. **Batch size**: Start with 100, increase to 200-500 if stable
2. **Create collection first**: The tool requires the collection to exist
3. **Monitor memory**: Watch Qdrant node RAM during migration
4. **Resumable**: Uses `_migration_offsets` collection to track progress

## Troubleshooting

### "target collection does not exist"
Create the Qdrant collection first (see setup section above).

### "property not found" error
Check that properties in `--weaviate.include-properties` exist in your Weaviate class schema.

### Out of memory during migration
Reduce `--migration.batch-size` (try 50 or 25).

## Differences from rossAI-migrator

| Feature | This Tool | rossAI-migrator |
|---------|-----------|-----------------|
| Source of truth | Manifest/offsets | S3 parquet + manifest |
| Resumability | Yes (via offsets collection) | Yes (via manifest) |
| Schema transformation | Property filtering only | Full custom mapping |
| Intermediate storage | None (direct migration) | S3 (staged) |
| Parallel instances | No | Yes (5+ instances) |
| Best for | Direct migrations | Large-scale (1B+ points) |

## Contributing Back

If you want to contribute this feature to upstream Qdrant:

```bash
# Create a branch
git checkout -b feat/weaviate-property-filtering

# Commit your changes
git add pkg/commons/config.go cmd/migrate_from_weaviate.go
git commit -m "feat: add property filtering for Weaviate migration"

# Push and create PR
git push origin feat/weaviate-property-filtering
```

## License

Same as upstream: Apache 2.0
