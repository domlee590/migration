# Migration Recovery - Step by Step

## Your Situation
- ✅ Successfully migrated **204,165,796 objects** to Qdrant (63.49% complete)
- ❌ Checkpoint was overwritten (shows 5,000 instead of 204M)
- 🎯 Need to migrate remaining **117,392,260 objects** (36.51%)
- 📊 Total: **321,558,056 objects** in Weaviate

## The Problem
Weaviate uses **cursor-based pagination** - you need the **UUID** at position 204,165,796 to resume.

## Solution: 2-Step Process

### Step 1: Find the UUID (1-3 hours)

Run the optimized script to paginate through Weaviate and find the UUID:

```bash
cd /Users/vecflow/Workspace2/migration

# Use screen/tmux to keep it running
screen -S weaviate-find

python find_weaviate_cursor_optimized.py 204165796

# Ctrl+A, D to detach
# screen -r weaviate-find to reattach
```

**Note**: Configuration is loaded from `.env` file (WEAVIATE_URL, WEAVIATE_API_KEY, WEAVIATE_COLLECTION)

**What it does:**
- Paginates through Weaviate in batches of 10,000 (max)
- Skips vectors (huge performance improvement)
- Shows progress every 10 seconds
- Supports resume if interrupted
- Expected time: **1-3 hours**

**Output:**
```
✅ SUCCESS - UUID FOUND!
Weaviate UUID at position 204,165,796: abc-123-def-456
```

### Step 2: Update the Checkpoint (instant)

Use the UUID from Step 1 to update the checkpoint:

```bash
python update_checkpoint.py 'abc-123-def-456' 204165796
```

Replace `'abc-123-def-456'` with the actual UUID from Step 1.

**Note**: Configuration is loaded from `.env` file (QDRANT_URL, QDRANT_API_KEY, etc.)

**What it does:**
- Connects to Qdrant
- Updates the `_migration_offsets` collection
- Sets the checkpoint to resume from position 204,165,796
- Matches the exact format the Go migration tool expects

**Output:**
```
✓ Checkpoint updated successfully!
✓ Checkpoint verified:
    Text_tables_offset: abc-123-def-456
    Text_tables_offsetCount: 204165796
    Text_tables_lastUpsertAt: 2026-01-28T...
```

### Step 3: Resume Migration

Now run the migration tool - it will automatically resume from the checkpoint.

Check your migration script or command to see how you run it normally. The tool will:
- Read the checkpoint from `_migration_offsets` collection
- Use the UUID as the cursor to resume from position 204,165,796
- Continue migrating the remaining 117,392,260 objects

**⚠️ CRITICAL: Do NOT use `--migration.restart` flag!**
That would overwrite your checkpoint again.

## How the Checkpoint Works

The checkpoint is stored in Qdrant's `_migration_offsets` collection:

```json
{
  "point_id": "2e09f706-a455-57d2-80e5-9b1d80f0ce6d",  // Deterministic UUID
  "payload": {
    "Text_tables_offset": "abc-123-def-456",        // Weaviate cursor UUID
    "Text_tables_offsetCount": 204165796,             // Items processed
    "Text_tables_lastUpsertAt": "2026-01-28T..."     // Timestamp
  }
}
```

**Point ID Generation:**
```python
# Deterministic UUID v5 (SHA-1) from class name
namespace_url = uuid.UUID('6ba7b811-9dad-11d1-80b4-00c04fd430c8')
point_id = uuid.uuid5(namespace_url, "Text_tables")
# Result: 2e09f706-a455-57d2-80e5-9b1d80f0ce6d
```

This matches the Go code in `pkg/commons/offsets.go`.

## Understanding the Migration Tool

From `cmd/migrate_from_weaviate.go`:

1. **On startup**, it calls `GetStartOffset()` to retrieve the checkpoint
2. **Uses the UUID** as the `after` parameter for cursor pagination:
   ```go
   query = query.WithAfter(offsetID.GetUuid())
   ```
3. **After each batch**, it updates the checkpoint with the new UUID and count
4. **If no checkpoint exists**, it starts from the beginning

## Files Created

- **`find_weaviate_cursor_optimized.py`** - Find UUID at position (1-3 hours)
- **`update_checkpoint.py`** - Update Qdrant checkpoint (instant)
- **`RECOVERY_STEPS.md`** - This file

## Troubleshooting

**Q: What if the script gets interrupted?**
A: It prints a resume command - just run it to continue from where it stopped.

**Q: How do I verify the checkpoint was updated?**
A: The `update_checkpoint.py` script verifies it automatically and shows the payload.

**Q: Can I check the checkpoint manually?**
A: Yes, using Python:
```python
from qdrant_client import QdrantClient
client = QdrantClient(url="...", api_key="...")
points = client.scroll(collection_name="_migration_offsets", limit=10)[0]
for p in points:
    print(p.payload)
```

**Q: What if I accidentally run with `--migration.restart`?**
A: It will overwrite the checkpoint again. Just repeat Steps 1-2 to fix it.

## Time Estimates

- **Step 1 (Find UUID)**: 1-3 hours
- **Step 2 (Update checkpoint)**: < 1 second  
- **Step 3 (Finish migration)**: ~3-4 hours for remaining 117M objects

**Total**: ~4-7 hours to complete the migration

## Prevention for Next Time

1. **Use screen/tmux** - keeps session alive if SSH disconnects
2. **Enable logging** - capture all output to a file:
   ```bash
   ./bin/qdrant-migration weaviate [args...] 2>&1 | tee migration.log
   ```
3. **Monitor checkpoint** - periodically check it's updating
4. **Don't use `--migration.restart`** unless you really want to start over
