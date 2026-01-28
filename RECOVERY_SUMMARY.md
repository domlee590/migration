# Migration Checkpoint Recovery - Quick Summary

## What Happened
- ✅ You migrated **204,165,796 objects** to Qdrant (63.49% complete)
- ❌ Checkpoint was overwritten (now shows 5,000 instead of 204M)
- 🎯 Need UUID at position 204,165,796 to resume

## 2-Step Recovery

### Step 1: Find the UUID (~1-3 hours)
```bash
cd /Users/vecflow/Workspace2/migration
screen -S weaviate-find
python find_weaviate_cursor_optimized.py 204165796
```

Detach: `Ctrl+A, D` | Reattach: `screen -r weaviate-find`

**If interrupted**, it will print a resume command like:
```bash
python find_weaviate_cursor_optimized.py 204165796 \
  --resume-from 'uuid-here' \
  --resume-position 150000000
```

### Step 2: Update Checkpoint (instant)
When Step 1 completes with UUID (e.g., `abc-123-def`):
```bash
python update_checkpoint.py 'abc-123-def' 204165796
```

### Step 3: Resume Migration
Run your normal migration command WITHOUT `--migration.restart` flag.

The tool will automatically:
- Read the checkpoint from `_migration_offsets`
- Resume from position 204,165,796
- Migrate the remaining 117,392,260 objects

## How It Works

### Checkpoint Format
Stored in Qdrant's `_migration_offsets` collection:
```json
{
  "Text_tables_offset": "uuid-here",        // Weaviate cursor
  "Text_tables_offsetCount": 204165796,     // Items processed
  "Text_tables_lastUpsertAt": "2026-01-28..." // Timestamp
}
```

### Point ID Generation
Uses deterministic UUID v5 (SHA-1) from class name:
```python
namespace = uuid.UUID('6ba7b811-9dad-11d1-80b4-00c04fd430c8')  # NameSpaceURL
point_id = uuid.uuid5(namespace, "Text_tables")
```

This matches the Go code in `pkg/commons/offsets.go`.

## Configuration

Both scripts load from `.env`:
- `WEAVIATE_URL`
- `WEAVIATE_API_KEY`
- `WEAVIATE_COLLECTION` (defaults to "Text_tables")
- `QDRANT_URL`
- `QDRANT_API_KEY`
- `QDRANT_COLLECTION` (defaults to "chunks")

## Optimizations

The find script uses:
- ⚡ 10,000 objects/batch (maximum)
- ⚡ No vector fetching
- ⚡ Minimal properties (only UUIDs)
- ⚡ Cursor pagination

**Result: 3-5x faster than naive approach (1-3 hrs vs 5-10 hrs)**

## Files

- `find_weaviate_cursor_optimized.py` - Find UUID at position
- `update_checkpoint.py` - Update checkpoint in Qdrant
- `RECOVERY_STEPS.md` - Detailed documentation
- `RECOVERY_SUMMARY.md` - This file
