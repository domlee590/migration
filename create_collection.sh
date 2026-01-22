#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Qdrant Cloud Configuration
# -----------------------------
# Cluster: 4 nodes × 32GB RAM × 512GB SSD × 8 vCPU
# Region: us-east-1
#
# RAM note: 320M vectors × 1024-dim INT8 = ~160GB/node (with RF=2)
# 32GB RAM < 160GB needed, so we use mmap (always_ram=false)
# OS page cache will keep hot data in RAM for filtered queries
# -----------------------------
QDRANT_API_KEY="YOUR_API_KEY_HERE"
QDRANT_BASE_URL="YOUR_URL_HERE"
COLLECTION_NAME="chunks"

COMMON_HEADERS=(
  -H "api-key: ${QDRANT_API_KEY}"
  -H "Content-Type: application/json"
)

# -----------------------------
# Create collection with production settings
# -----------------------------
# - 8 shards (2× nodes for even distribution + scaling headroom)
# - 2× replication for HA (survive 1 node failure)
# - Scalar INT8 quantization with mmap (OS page cache for hot data)
# - Original vectors on disk (rarely accessed after quantization)
# - HNSW graph in RAM for fast traversal
# - Payloads on disk (lean payload, only filter fields)
# -----------------------------
echo "Creating collection: ${COLLECTION_NAME}..."

curl -sS -X PUT \
  "${COMMON_HEADERS[@]}" \
  "${QDRANT_BASE_URL}/collections/${COLLECTION_NAME}" \
  -d '{
    "vectors": {
      "size": 1024,
      "distance": "Cosine",
      "on_disk": true
    },
    "shard_number": 8,
    "replication_factor": 2,
    "write_consistency_factor": 1,
    "on_disk_payload": true,
    "hnsw_config": {
      "m": 16,
      "ef_construct": 100,
      "on_disk": false
    },
    "quantization_config": {
      "scalar": {
        "type": "int8",
        "quantile": 0.99,
        "always_ram": false
      }
    },
    "optimizers_config": {
      "indexing_threshold": 20000,
      "memmap_threshold": 50000
    }
  }'

echo ""
echo "✅ Created collection: ${COLLECTION_NAME}"

# -----------------------------
# Create payload indexes for filtering
# -----------------------------
# These indexes are critical for multi-tenant isolation
# 90%+ queries filter by doc_id, 10% by data_source_id
# -----------------------------
echo "Creating payload indexes..."

curl -sS -X PUT \
  "${COMMON_HEADERS[@]}" \
  "${QDRANT_BASE_URL}/collections/${COLLECTION_NAME}/index" \
  -d '{
    "field_name": "doc_id",
    "field_schema": "keyword"
  }'

echo ""
echo "✅ Created index: doc_id (keyword)"

curl -sS -X PUT \
  "${COMMON_HEADERS[@]}" \
  "${QDRANT_BASE_URL}/collections/${COLLECTION_NAME}/index" \
  -d '{
    "field_name": "data_source_id",
    "field_schema": "keyword"
  }'

echo ""
echo "✅ Created index: data_source_id (keyword)"

# -----------------------------
# Verify collection info
# -----------------------------
echo ""
echo "Collection info:"
curl -sS -X GET \
  "${COMMON_HEADERS[@]}" \
  "${QDRANT_BASE_URL}/collections/${COLLECTION_NAME}" | python3 -m json.tool 2>/dev/null || cat

echo ""
echo "🎉 Done. Collection ready for migration."
