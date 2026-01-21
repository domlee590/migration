#!/bin/bash
set -e

echo "Building modified Qdrant migration tool..."

# Use full path to go binary, or find it in PATH
if [ -n "$GO_BIN" ]; then
    # Use user-provided GO_BIN
    :
elif command -v go >/dev/null 2>&1; then
    # Use go from PATH
    GO_BIN=$(command -v go)
elif [ -x "/usr/local/go/bin/go" ]; then
    # Try standard location
    GO_BIN="/usr/local/go/bin/go"
else
    echo "Error: Go not found. Set GO_BIN environment variable to your go binary path"
    exit 1
fi

echo "Using Go at: $GO_BIN"

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
