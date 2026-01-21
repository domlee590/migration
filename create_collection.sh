#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Constants (placeholders)
# -----------------------------
QDRANT_API_KEY="YOUR_API_KEY_HERE"
QDRANT_BASE_URL="YOUR_URL_HERE"
COLLECTION_NAME="chunks"

VECTOR_SIZE=1024
VECTOR_DISTANCE="Cosine"
VECTORS_ON_DISK=true

# Common headers (bash array so quoting is correct)
COMMON_HEADERS=(
  -H "api-key: ${QDRANT_API_KEY}"
  -H "Content-Type: application/json"
)

# -----------------------------
# Create collection
# -----------------------------
curl -sS -X PUT \
  "${COMMON_HEADERS[@]}" \
  "${QDRANT_BASE_URL}/collections/${COLLECTION_NAME}" \
  -d "{
    \"vectors\": {
      \"size\": ${VECTOR_SIZE},
      \"distance\": \"${VECTOR_DISTANCE}\",
      \"on_disk\": ${VECTORS_ON_DISK}
    }
  }"

echo "✅ Created/updated collection: ${COLLECTION_NAME}"

# -----------------------------
# Create indexes
# -----------------------------
curl -sS -X PUT \
  "${COMMON_HEADERS[@]}" \
  "${QDRANT_BASE_URL}/collections/${COLLECTION_NAME}/index" \
  -d '{
    "field_name": "doc_id",
    "field_schema": "keyword"
  }'

echo "✅ Created index: doc_id (keyword)"

curl -sS -X PUT \
  "${COMMON_HEADERS[@]}" \
  "${QDRANT_BASE_URL}/collections/${COLLECTION_NAME}/index" \
  -d '{
    "field_name": "data_source_id",
    "field_schema": "keyword"
  }'

echo "✅ Created index: data_source_id (keyword)"

echo "🎉 Done."
