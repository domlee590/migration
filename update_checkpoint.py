#!/usr/bin/env python
"""
Update Migration Checkpoint in Qdrant

After finding the Weaviate UUID at your target position, use this script to
update the checkpoint in Qdrant's _migration_offsets collection.

This matches the exact format used by the Go migration tool in:
pkg/commons/offsets.go (StoreStartOffset function)

USAGE:
  python update_checkpoint.py <weaviate_uuid> <count>

EXAMPLE:
  python update_checkpoint.py 'abc-123-def-456' 204165796
"""

import sys
import uuid
import os
from datetime import datetime
from pathlib import Path

try:
    from qdrant_client import QdrantClient
    from qdrant_client.models import PointStruct
except ImportError:
    print("Error: qdrant-client not installed")
    print("Install with: pip install qdrant-client")
    sys.exit(1)

try:
    from dotenv import load_dotenv
except ImportError:
    print("Error: python-dotenv not installed")
    print("Install with: pip install python-dotenv")
    sys.exit(1)

# Load environment variables from .env file
env_path = Path(__file__).parent / ".env"
load_dotenv(env_path)

# Configuration (from .env)
QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "chunks")
WEAVIATE_CLASS = os.getenv("WEAVIATE_COLLECTION", "Text_tables")
OFFSETS_COLLECTION = "_migration_offsets"


def generate_checkpoint_point_id(source_collection: str) -> str:
    """
    Generate deterministic point ID for checkpoint.

    This MUST match the Go code's algorithm:
    uuid.NewSHA1(uuid.NameSpaceURL, []byte(sourceCollection))

    Args:
        source_collection: Weaviate class name (e.g., "Text_tables")

    Returns:
        UUID string for the checkpoint point
    """
    # UUID namespace for URLs (same as Go's uuid.NameSpaceURL)
    namespace_url = uuid.UUID("6ba7b811-9dad-11d1-80b4-00c04fd430c8")

    # Generate UUID v5 (SHA-1 based) - same as Go's uuid.NewSHA1
    checkpoint_uuid = uuid.uuid5(namespace_url, source_collection)

    return str(checkpoint_uuid)


def update_checkpoint(weaviate_uuid: str, offset_count: int):
    """
    Update the migration checkpoint in Qdrant.

    Args:
        weaviate_uuid: Weaviate UUID to resume from (the cursor)
        offset_count: Number of items processed
    """
    print("=" * 80)
    print("UPDATE MIGRATION CHECKPOINT")
    print("=" * 80)

    # Validate environment variables
    if not QDRANT_URL:
        print("✗ Error: QDRANT_URL not found in .env file")
        return 1
    if not QDRANT_API_KEY:
        print("✗ Error: QDRANT_API_KEY not found in .env file")
        return 1

    # Validate UUID format
    try:
        uuid.UUID(weaviate_uuid)
    except ValueError:
        print(f"✗ Invalid UUID format: {weaviate_uuid}")
        print(f"  UUIDs should look like: 'abc12345-1234-5678-90ab-cdef12345678'")
        return 1

    print(f"\n📋 Configuration (from .env):")
    print(f"  Qdrant URL: {QDRANT_URL}")
    print(f"  Qdrant Collection: {QDRANT_COLLECTION}")
    print(f"  Offsets Collection: {OFFSETS_COLLECTION}")
    print(f"  Weaviate Class: {WEAVIATE_CLASS}")

    print(f"\n📝 Checkpoint Data:")
    print(f"  Weaviate UUID (cursor): {weaviate_uuid}")
    print(f"  Offset Count: {offset_count:,}")

    # Generate deterministic point ID (must match Go code)
    checkpoint_point_id = generate_checkpoint_point_id(WEAVIATE_CLASS)
    print(f"  Checkpoint Point ID: {checkpoint_point_id}")

    # Connect to Qdrant
    print(f"\n🔌 Connecting to Qdrant...")
    client = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY, timeout=60)
    print("✓ Connected successfully")

    # Ensure _migration_offsets collection exists
    try:
        client.get_collection(OFFSETS_COLLECTION)
        print(f"✓ Collection '{OFFSETS_COLLECTION}' exists")
    except Exception:
        print(f"Creating collection '{OFFSETS_COLLECTION}'...")
        client.create_collection(
            collection_name=OFFSETS_COLLECTION,
            vectors_config={},  # No vectors needed, just payload storage
        )
        print(f"✓ Created collection '{OFFSETS_COLLECTION}'")

    # Create checkpoint payload (must match Go code format)
    # From pkg/commons/offsets.go:
    # sourceCollection + "_offset": offsetId
    # sourceCollection + "_offsetCount": offsetCount
    # sourceCollection + "_lastUpsertAt": time.Now().Format(time.RFC3339)

    timestamp = datetime.utcnow().isoformat() + "Z"

    payload = {
        f"{WEAVIATE_CLASS}_offset": weaviate_uuid,
        f"{WEAVIATE_CLASS}_offsetCount": offset_count,
        f"{WEAVIATE_CLASS}_lastUpsertAt": timestamp,
    }

    print(f"\n💾 Upserting checkpoint...")
    print(f"  Payload keys: {list(payload.keys())}")

    # Upsert checkpoint (same as Go code)
    client.upsert(
        collection_name=OFFSETS_COLLECTION,
        points=[
            PointStruct(
                id=checkpoint_point_id, vector={}, payload=payload  # Empty vector map
            )
        ],
        wait=True,
    )

    print(f"✓ Checkpoint updated successfully!")

    # Verify the checkpoint
    print(f"\n🔍 Verifying checkpoint...")
    points = client.retrieve(
        collection_name=OFFSETS_COLLECTION, ids=[checkpoint_point_id], with_payload=True
    )

    if points:
        point = points[0]
        print(f"✓ Checkpoint verified:")
        for key, value in point.payload.items():
            print(f"    {key}: {value}")

    print("\n" + "=" * 80)
    print("✅ CHECKPOINT UPDATE COMPLETE")
    print("=" * 80)
    print(f"\n📖 Next Steps:")
    print(f"  1. The migration tool will now resume from position {offset_count:,}")
    print(f"  2. It will use Weaviate UUID '{weaviate_uuid}' as the cursor")
    print(f"  3. Run your migration command (WITHOUT --migration.restart flag)")
    print(
        f"\n⚠️  IMPORTANT: Do NOT use --migration.restart (it will overwrite the checkpoint!)"
    )
    print("\n💡 The migration tool will:")
    print(f"  - Read the checkpoint from '{OFFSETS_COLLECTION}' collection")
    print(f"  - Continue from UUID '{weaviate_uuid}' (position {offset_count:,})")
    print(f"  - Migrate the remaining objects to '{QDRANT_COLLECTION}' collection")
    print("=" * 80)

    return 0


def main():
    if len(sys.argv) < 3:
        print("=" * 80)
        print("UPDATE MIGRATION CHECKPOINT")
        print("=" * 80)
        print(
            "\nUpdate the Qdrant migration checkpoint with a Weaviate UUID and count."
        )
        print("\nUSAGE:")
        print("  python update_checkpoint.py <weaviate_uuid> <count>")
        print("\nEXAMPLE:")
        print("  python update_checkpoint.py 'abc-123-def-456' 204165796")
        print("\nPARAMETERS:")
        print("  weaviate_uuid : The Weaviate UUID to resume from (cursor)")
        print("  count         : Number of items processed (for progress tracking)")
        print("\nNOTE:")
        print("  Configuration is loaded from .env file:")
        print("    - QDRANT_URL")
        print("    - QDRANT_API_KEY")
        print("    - QDRANT_COLLECTION (defaults to 'chunks')")
        print("    - WEAVIATE_COLLECTION (defaults to 'Text_tables')")
        print(
            "\n  This script matches the checkpoint format used by the Go migration tool."
        )
        print("  The checkpoint is stored in the '_migration_offsets' collection.")
        print("=" * 80)
        return 1

    weaviate_uuid = sys.argv[1]
    offset_count = int(sys.argv[2])

    return update_checkpoint(weaviate_uuid, offset_count)


if __name__ == "__main__":
    sys.exit(main())
