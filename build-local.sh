#!/bin/bash
set -e

echo "Building modified Qdrant migration tool..."

# Use full path to go binary
GO_BIN=${GO_BIN:-/usr/local/go/bin/go}

if [ ! -x "$GO_BIN" ]; then
    echo "Error: Go not found at $GO_BIN"
    echo "Set GO_BIN environment variable to your go binary path"
    exit 1
fi

# Build the binary locally
CGO_ENABLED=1 $GO_BIN build -ldflags "-X 'main.projectVersion=custom' -X 'main.projectBuild=lean-payload'" -o bin/qdrant-migration main.go

echo "✅ Build complete! Binary: ./bin/qdrant-migration"
echo ""
echo "Usage example for lean migration (only doc_id, data_source_id):"
echo ""
echo "./bin/qdrant-migration weaviate \\"
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
