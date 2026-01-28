#!/usr/bin/env python
"""
OPTIMIZED Weaviate Cursor Finder
Find the UUID of an object at a specific position in Weaviate.

PURPOSE:
When a Weaviate → Qdrant migration crashes and the checkpoint is lost, you need
the Weaviate UUID at the position where you left off to resume the migration.

PROBLEM:
- Weaviate uses cursor-based pagination (not numeric offsets)
- You can't jump to position 204M without iterating through all previous items
- The checkpoint stores the UUID + count (UUID is the cursor, count is for display)

SOLUTION:
This script efficiently paginates through Weaviate to find the UUID at your target position.

KEY OPTIMIZATIONS (3-5x faster than naive approach):
- Uses maximum batch size of 10,000 (Weaviate's QUERY_MAXIMUM_RESULTS limit)
- Does NOT fetch vectors (massive performance improvement - vectors are expensive)
- Only fetches minimal properties (just UUIDs - no other data needed)
- Uses cursor-based pagination with 'after' parameter (no performance degradation)

PERFORMANCE:
For 204,165,796 objects:
- ~20,400 batch requests needed
- Expected time: 1-3 hours (vs 5-10 hours with small batch sizes)
- At 0.2s/request: ~68 minutes
- At 0.5s/request: ~170 minutes

USAGE:
  python find_weaviate_cursor_optimized.py 204165796

Configuration is loaded from .env file:
  - WEAVIATE_URL
  - WEAVIATE_API_KEY
  - WEAVIATE_COLLECTION (defaults to "Text_tables")

RESEARCH FINDINGS:
- Maximum batch size: 10,000 (Weaviate's QUERY_MAXIMUM_RESULTS default limit)
- Don't increase QUERY_MAXIMUM_RESULTS - it hurts performance
- Offset-based pagination gets exponentially slower with large offsets
- Cursor-based pagination maintains constant performance at any scale
- Fetching vectors is the #1 performance bottleneck - always disable for iteration
"""

import sys
import time
import os
from datetime import datetime, timedelta
from pathlib import Path

try:
    import weaviate
    import weaviate.classes as wvc
except ImportError:
    print("Error: weaviate-client not installed")
    print("Install with: pip install weaviate-client")
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

# Load configuration from environment
WEAVIATE_URL = os.getenv("WEAVIATE_URL")
WEAVIATE_API_KEY = os.getenv("WEAVIATE_API_KEY")
WEAVIATE_CLASS = os.getenv("WEAVIATE_COLLECTION", "Text_tables")

# OPTIMAL BATCH SIZE: Maximum allowed by Weaviate
OPTIMAL_BATCH_SIZE = 10000


def find_uuid_at_position(
    weaviate_url: str,
    api_key: str,
    class_name: str,
    target_position: int,
    batch_size: int = OPTIMAL_BATCH_SIZE,
    resume_from_uuid: str = None,
    resume_position: int = 0,
):
    """
    Find the Weaviate UUID at a specific position using OPTIMIZED settings.

    Args:
        weaviate_url: Weaviate host (e.g., 'https://your-cluster.weaviate.cloud')
        api_key: Weaviate API key
        class_name: Weaviate class name
        target_position: Target position (e.g., 204,165,796)
        batch_size: Objects per batch (max 10,000, default: 10,000)
        resume_from_uuid: Optional UUID to resume from
        resume_position: Position to resume from (if resuming)
    """
    print("=" * 80)
    print("OPTIMIZED WEAVIATE CURSOR FINDER")
    print("=" * 80)
    print(f"\n🎯 Target position: {target_position:,}")
    print(f"📦 Batch size: {batch_size:,} objects (MAX PERFORMANCE)")
    print(f"⚡ Optimizations: No vectors, minimal properties, cursor pagination")

    if resume_from_uuid:
        print(f"🔄 Resuming from UUID: {resume_from_uuid}")
        print(f"🔄 Starting position: {resume_position:,}")

    # Calculate ETA
    total_batches = (target_position - resume_position) // batch_size
    print(f"\n📊 Expected ~{total_batches:,} batch requests")
    print(f"📊 At 0.2s/request: ~{total_batches * 0.2 / 60:.1f} minutes")
    print(f"📊 At 0.5s/request: ~{total_batches * 0.5 / 60:.1f} minutes")

    # Connect to Weaviate
    print(f"\n🔌 Connecting to Weaviate...")
    client = None
    try:
        client = weaviate.connect_to_weaviate_cloud(
            cluster_url=weaviate_url,
            auth_credentials=wvc.init.Auth.api_key(api_key),
        )

        collection = client.collections.get(class_name)
        print(f"✅ Connected successfully")
        print(f"✅ Collection '{class_name}' found")

    except Exception as e:
        print(f"❌ Connection failed: {e}")
        if client:
            client.close()
        return None

    # Start pagination
    current_position = resume_position
    cursor = resume_from_uuid
    start_time = time.time()
    last_print_time = start_time
    last_position = resume_position
    batch_count = 0

    print(f"\n🚀 Starting optimized pagination...")
    print("=" * 80)

    try:
        while current_position < target_position:
            batch_start_time = time.time()

            # Query batch - OPTIMIZED: no vectors, minimal properties
            try:
                if cursor:
                    result = collection.query.fetch_objects(
                        limit=batch_size,
                        after=cursor,
                        include_vector=False,  # CRITICAL: Don't fetch vectors!
                        return_properties=[],  # CRITICAL: No properties needed, just UUID
                    )
                else:
                    result = collection.query.fetch_objects(
                        limit=batch_size, include_vector=False, return_properties=[]
                    )
            except Exception as e:
                print(f"\n❌ Query failed at position {current_position:,}: {e}")
                print(f"Last known cursor: {cursor}")
                return None

            batch_time = time.time() - batch_start_time

            # Extract objects
            objects = result.objects

            if not objects:
                print(f"\n⚠️  No more objects found at position {current_position:,}")
                print(f"Total objects in Weaviate: {current_position:,}")
                return None

            # Update cursor to last object in batch
            last_object = objects[-1]
            cursor = str(last_object.uuid)
            objects_fetched = len(objects)
            current_position += objects_fetched
            batch_count += 1

            # Print progress every 10 seconds OR every 10 batches (whichever comes first)
            current_time = time.time()
            should_print = (current_time - last_print_time >= 10) or (
                batch_count % 10 == 0
            )

            if should_print:
                elapsed = current_time - start_time
                objects_processed = current_position - resume_position
                rate = objects_processed / elapsed if elapsed > 0 else 0
                remaining_objects = target_position - current_position
                remaining_time = remaining_objects / rate if rate > 0 else 0
                eta = datetime.now() + timedelta(seconds=remaining_time)

                # Calculate recent batch performance
                recent_rate = (current_position - last_position) / (
                    current_time - last_print_time
                )

                print(
                    f"📈 Position: {current_position:,} / {target_position:,} "
                    f"({100 * current_position / target_position:.2f}%)"
                )
                print(
                    f"   Batches: {batch_count:,} | Last batch: {objects_fetched:,} objects in {batch_time:.2f}s"
                )
                print(
                    f"   Rate: {rate:,.0f} obj/sec | Recent: {recent_rate:,.0f} obj/sec"
                )
                print(
                    f"   ETA: {eta.strftime('%Y-%m-%d %H:%M:%S')} ({remaining_time/60:.1f} min remaining)"
                )
                print("-" * 80)

                last_print_time = current_time
                last_position = current_position

            # Check if we've reached or passed the target
            if current_position >= target_position:
                print(f"\n✅ REACHED TARGET POSITION!")
                print(f"Final position: {current_position:,}")
                print(f"Weaviate UUID at this position: {cursor}")
                print(f"Total batches: {batch_count:,}")
                print(f"Total time: {(time.time() - start_time)/60:.1f} minutes")
                return cursor

    except KeyboardInterrupt:
        print(f"\n\n⚠️  INTERRUPTED by user")
        print(f"Current position: {current_position:,}")
        print(f"Last known cursor: {cursor}")
        print(f"Batches completed: {batch_count:,}")
        print(f"\n💾 To resume from this position, run:")
        print(f"   python {sys.argv[0]} {target_position} \\")
        print(f"     --resume-from '{cursor}' \\")
        print(f"     --resume-position {current_position}")
        return None
    finally:
        if client:
            client.close()

    return cursor


def main():
    if len(sys.argv) < 2:
        print("=" * 80)
        print("OPTIMIZED WEAVIATE CURSOR FINDER")
        print("Find UUID at specific position using MAXIMUM PERFORMANCE settings")
        print("=" * 80)
        print("\nConfiguration loaded from .env:")
        print(f"  WEAVIATE_URL: {WEAVIATE_URL or 'NOT SET'}")
        print(f"  WEAVIATE_COLLECTION: {WEAVIATE_CLASS or 'NOT SET'}")
        print("\nUsage:")
        print("  python find_weaviate_cursor_optimized.py <position> [options]")
        print("\nExample:")
        print("  python find_weaviate_cursor_optimized.py 204165796")
        print("\nOptions:")
        print(
            "  --batch-size N          : Objects per batch (default: 10000, max: 10000)"
        )
        print("  --resume-from UUID      : Resume from this UUID")
        print("  --resume-position N     : Starting position when resuming")
        print("\nNote: Weaviate credentials are loaded from .env file")
        print("      Make sure .env exists with WEAVIATE_URL and WEAVIATE_API_KEY")
        print("\n" + "=" * 80)
        print("PERFORMANCE NOTES:")
        print("=" * 80)
        print("\n✨ OPTIMIZATIONS ENABLED:")
        print("  • Maximum batch size: 10,000 objects/request")
        print("  • No vector fetching (huge performance gain)")
        print("  • Minimal property fetching (only UUIDs)")
        print("  • Cursor-based pagination (no performance degradation)")
        print("\n⏱️  EXPECTED TIME FOR 204M OBJECTS:")
        print("  • ~20,400 batch requests needed")
        print("  • At 0.2s/request: ~68 minutes (~1.1 hours)")
        print("  • At 0.5s/request: ~170 minutes (~2.8 hours)")
        print("\n💡 TIP: Run in screen/tmux for long-running operations:")
        print("  screen -S weaviate-cursor")
        print("  python find_weaviate_cursor_optimized.py 204165796")
        print("  # Ctrl+A, D to detach")
        print("  # screen -r weaviate-cursor to reattach")
        return 1

    # Validate environment variables
    if not WEAVIATE_URL:
        print("✗ Error: WEAVIATE_URL not found in .env file")
        return 1
    if not WEAVIATE_API_KEY:
        print("✗ Error: WEAVIATE_API_KEY not found in .env file")
        return 1

    target_position = int(sys.argv[1])

    # Parse optional arguments
    batch_size = OPTIMAL_BATCH_SIZE
    resume_from = None
    resume_position = 0

    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == "--batch-size" and i + 1 < len(sys.argv):
            batch_size = min(int(sys.argv[i + 1]), OPTIMAL_BATCH_SIZE)
            if batch_size < OPTIMAL_BATCH_SIZE:
                print(
                    f"⚠️  Warning: Using batch size {batch_size} (max is {OPTIMAL_BATCH_SIZE})"
                )
            i += 2
        elif sys.argv[i] == "--resume-from" and i + 1 < len(sys.argv):
            resume_from = sys.argv[i + 1]
            i += 2
        elif sys.argv[i] == "--resume-position" and i + 1 < len(sys.argv):
            resume_position = int(sys.argv[i + 1])
            i += 2
        else:
            i += 1

    cursor = find_uuid_at_position(
        WEAVIATE_URL,
        WEAVIATE_API_KEY,
        WEAVIATE_CLASS,
        target_position,
        batch_size,
        resume_from,
        resume_position,
    )

    if cursor:
        print("\n" + "=" * 80)
        print("✅ SUCCESS - UUID FOUND!")
        print("=" * 80)
        print(f"\n📋 Results:")
        print(f"  Weaviate UUID at position {target_position:,}: {cursor}")
        print(f"  Class: {WEAVIATE_CLASS}")
        print(f"\n📝 NEXT STEP: Update the checkpoint in Qdrant")
        print(f"\n  python update_checkpoint.py '{cursor}' {target_position}")
        print("\n  This will update the _migration_offsets collection so the migration")
        print("  tool knows to resume from this position.")
        print("\n" + "=" * 80)
        return 0
    else:
        return 1


if __name__ == "__main__":
    sys.exit(main())
