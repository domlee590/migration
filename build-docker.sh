#!/bin/bash
set -e

IMAGE_NAME="qdrant-migration-lean"
TAG="latest"

echo "Building Docker image with lean payload support..."

docker build \
  --build-arg VERSION=custom \
  --build-arg BUILD=lean-payload \
  -t ${IMAGE_NAME}:${TAG} \
  .

echo "✅ Docker build complete! Image: ${IMAGE_NAME}:${TAG}"
echo ""
echo "Usage example:"
echo ""
echo "docker run --rm -it ${IMAGE_NAME}:${TAG} weaviate \\"
echo "  --weaviate.host 'your-weaviate-host' \\"
echo "  --weaviate.scheme 'https' \\"
echo "  --weaviate.auth-type 'apiKey' \\"
echo "  --weaviate.api-key 'your-api-key' \\"
echo "  --weaviate.class-name 'Text_tables' \\"
echo "  --weaviate.include-properties 'doc_id,data_source_id' \\"
echo "  --qdrant.url 'https://your-qdrant-url:6334' \\"
echo "  --qdrant.api-key 'your-qdrant-key' \\"
echo "  --qdrant.collection 'chunks' \\"
echo "  --migration.batch-size 100"
