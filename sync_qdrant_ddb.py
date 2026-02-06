#!/usr/bin/env python
"""
DDB-Qdrant Chunk Sync Script

Scans all chunks in DynamoDB and all points in Qdrant, identifies discrepancies,
and syncs them: embeds + upserts missing chunks to Qdrant, deletes orphan points.

Uses disk-based processing (sorted files + OS sort/comm) to stay memory-efficient
on a t3.2xlarge (32GB RAM, 8 vCPU).

Usage (run phases sequentially):
    python sync_qdrant_ddb.py --phase scan      # Concurrent DDB + Qdrant scan to disk
    python sync_qdrant_ddb.py --phase diff       # Sort and compute missing/orphan sets
    python sync_qdrant_ddb.py --phase embed      # Embed missing chunks + upsert (confirms)
    python sync_qdrant_ddb.py --phase delete     # Delete orphan points (confirms)

Requires .env in same directory with:
    QDRANT_URL, QDRANT_API_KEY, QDRANT_COLLECTION,
    DDB_TABLE_NAME, DDB_REGION,
    VOYAGEAI_APIKEY,
    SUPABASE_URL, SUPABASE_KEY

AWS credentials from system (IAM role / env vars / ~/.aws).

Dependencies:
    pip install boto3 qdrant-client grpcio voyageai python-dotenv tqdm httpx
"""

import argparse
import asyncio
import json
import logging
import os
import platform
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import boto3
import httpx
import voyageai
from boto3.dynamodb.conditions import Key
from botocore.config import Config
from dotenv import load_dotenv
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import PointIdsList, PointStruct
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Paths — all working files in a subdirectory alongside the script
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent
ENV_FILE = SCRIPT_DIR / ".env"
WORK_DIR = SCRIPT_DIR / "_sync_qdrant_ddb_work"
LOG_FILE = WORK_DIR / "sync.log"

# Scan output files
DDB_RAW_FILE = WORK_DIR / "ddb_raw.tsv"
QDRANT_RAW_FILE = WORK_DIR / "qdrant_raw.tsv"

# Scan checkpoints
DDB_CHECKPOINT = WORK_DIR / "ddb_scan_checkpoint.json"
QDRANT_CHECKPOINT = WORK_DIR / "qdrant_scan_checkpoint.json"

# Diff intermediate files
DDB_IDS_RAW = WORK_DIR / "ddb_ids_raw.txt"
QDRANT_IDS_RAW = WORK_DIR / "qdrant_ids_raw.txt"
DDB_IDS_SORTED = WORK_DIR / "ddb_ids_sorted.txt"
QDRANT_IDS_SORTED = WORK_DIR / "qdrant_ids_sorted.txt"
DDB_RAW_BY_ID = WORK_DIR / "ddb_raw_by_id.tsv"
MISSING_ENRICHED = WORK_DIR / "missing_enriched.tsv"
MISSING_BY_DOC_UNSORTED = WORK_DIR / "missing_by_doc_unsorted.tsv"
DOC_DSID_RAW = WORK_DIR / "doc_dsid_raw.tsv"

# Diff output files (consumed by embed/delete phases)
MISSING_FILE = WORK_DIR / "missing_in_qdrant.txt"
ORPHANS_FILE = WORK_DIR / "orphans_in_qdrant.txt"
MISSING_BY_DOC = WORK_DIR / "missing_by_doc.tsv"
DOC_DSID_MAP = WORK_DIR / "doc_dsid_map.tsv"

# Embed/delete checkpoints
EMBED_CHECKPOINT = WORK_DIR / "embed_progress.json"
DELETE_CHECKPOINT = WORK_DIR / "delete_progress.json"

# ---------------------------------------------------------------------------
# Tuning constants
# ---------------------------------------------------------------------------
MAX_RETRIES = 10
INITIAL_BACKOFF = 2       # seconds
MAX_BACKOFF = 300         # 5 minutes
CHECKPOINT_INTERVAL = 50  # save every N batches

DDB_TOTAL_SEGMENTS = 256
DDB_SCAN_WORKERS = 64

QDRANT_SCROLL_BATCH = 10_000

EMBED_BATCH_SIZE = 128
EMBED_MAX_RETRIES = 5
EMBED_CONCURRENCY = 6

QDRANT_UPSERT_BATCH = 100
QDRANT_UPSERT_RETRIES = 5

QDRANT_DELETE_BATCH = 500
QDRANT_DELETE_RETRIES = 5

SUPABASE_BATCH_SIZE = 100

# ---------------------------------------------------------------------------
# Logging — file only so tqdm progress bars stay clean
# ---------------------------------------------------------------------------
WORK_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE)],
)
logger = logging.getLogger(__name__)

for _noisy in ["httpx", "httpcore", "urllib3", "qdrant_client", "voyageai", "grpc"]:
    logging.getLogger(_noisy).setLevel(logging.WARNING)


# ===========================================================================
# Main class
# ===========================================================================
class ChunkSyncer:
    """Scans DDB + Qdrant, embeds missing chunks, deletes orphans."""

    def __init__(self):
        self._load_environment()

    # -----------------------------------------------------------------------
    # Environment
    # -----------------------------------------------------------------------
    def _load_environment(self):
        if not ENV_FILE.exists():
            print(f"Error: .env not found at {ENV_FILE}")
            sys.exit(1)

        load_dotenv(ENV_FILE)

        self.qdrant_url = os.getenv("QDRANT_URL")
        self.qdrant_api_key = os.getenv("QDRANT_API_KEY")
        self.qdrant_collection = os.getenv("QDRANT_COLLECTION", "chunks")
        self.ddb_table_name = os.getenv("DDB_TABLE_NAME", "chunks")
        self.ddb_region = os.getenv("DDB_REGION", "us-east-1")
        self.voyage_api_key = os.getenv("VOYAGEAI_APIKEY")
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")

        required = {
            "QDRANT_URL": self.qdrant_url,
            "QDRANT_API_KEY": self.qdrant_api_key,
            "VOYAGEAI_APIKEY": self.voyage_api_key,
            "SUPABASE_URL": self.supabase_url,
            "SUPABASE_KEY": self.supabase_key,
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            print(f"Error: missing required env vars: {', '.join(missing)}")
            sys.exit(1)

        logger.info(
            f"Config: qdrant={self.qdrant_url}/{self.qdrant_collection} "
            f"ddb={self.ddb_table_name}@{self.ddb_region}"
        )
        print(f"Qdrant:   {self.qdrant_url} / {self.qdrant_collection}")
        print(f"DynamoDB: {self.ddb_table_name} @ {self.ddb_region}")

    # -----------------------------------------------------------------------
    # Subprocess helpers
    # -----------------------------------------------------------------------
    @staticmethod
    def _run_pipe(cmd, output_file):
        with open(output_file, "w") as f:
            result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"{cmd[0]} failed: {result.stderr}")

    def _sort_file(self, input_file, output_file, label,
                   unique=True, tab_key=None, delete_input=True):
        """External merge sort via OS sort command."""
        print(f"  [{label}] Sorting {input_file.name}...")
        logger.info(f"[{label}] Sorting {input_file} -> {output_file}")

        cmd = ["sort"]
        if unique:
            cmd.append("-u")
        if tab_key:
            cmd.extend(["-t\t", f"-k{tab_key}"])
        if platform.system() == "Linux":
            cmd.extend(["-S", "4G", "--parallel=8", "-T", str(WORK_DIR)])
        cmd.extend(["-o", str(output_file), str(input_file)])

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"sort failed for [{label}]: {result.stderr}")

        count = self._count_lines(output_file)
        print(f"  [{label}] Done: {count:,} entries")
        logger.info(f"[{label}] {count:,} entries in {output_file}")

        if delete_input and input_file != output_file:
            input_file.unlink(missing_ok=True)
        return count

    @staticmethod
    def _comm_diff(file1, file2, output_file):
        """Set difference via comm: lines in file1 not in file2."""
        with open(output_file, "w") as f:
            result = subprocess.run(
                ["comm", "-23", str(file1), str(file2)],
                stdout=f, stderr=subprocess.PIPE, text=True,
            )
        if result.returncode != 0:
            raise RuntimeError(f"comm failed: {result.stderr}")
        return ChunkSyncer._count_lines(output_file)

    @staticmethod
    def _count_lines(filepath):
        if not Path(filepath).exists():
            return 0
        result = subprocess.run(
            ["wc", "-l", str(filepath)], capture_output=True, text=True,
        )
        return int(result.stdout.strip().split()[0])

    # =====================================================================
    # PHASE: scan — DDB parallel scan
    # =====================================================================
    def _load_ddb_checkpoint(self):
        if DDB_CHECKPOINT.exists():
            try:
                data = json.loads(DDB_CHECKPOINT.read_text())
                segs = set(data.get("completed_segments", []))
                count = data.get("count", 0)
                tqdm.write(
                    f"[DDB] Resuming: {len(segs)}/{DDB_TOTAL_SEGMENTS} segments, "
                    f"{count:,} chunks so far"
                )
                return segs, count
            except Exception as e:
                logger.warning(f"[DDB] Bad checkpoint, starting fresh: {e}")
        return set(), 0

    def _save_ddb_checkpoint(self, completed, count):
        try:
            DDB_CHECKPOINT.write_text(json.dumps({
                "completed_segments": sorted(completed),
                "count": count,
                "timestamp": datetime.utcnow().isoformat(),
            }))
        except Exception as e:
            logger.warning(f"[DDB] Checkpoint save failed: {e}")

    def _scan_ddb_segment(self, segment, total_segments, file_lock, fh, pbar):
        """Scan one DDB segment with per-page retry. Returns chunk count."""
        config = Config(
            region_name=self.ddb_region,
            retries={"max_attempts": 10, "mode": "adaptive"},
            max_pool_connections=10,
        )
        table = boto3.resource("dynamodb", config=config).Table(self.ddb_table_name)

        count = 0
        last_key = None

        while True:
            kwargs = {
                "ProjectionExpression": "#i, doc_id",
                "ExpressionAttributeNames": {"#i": "id"},
                "Segment": segment,
                "TotalSegments": total_segments,
            }
            if last_key:
                kwargs["ExclusiveStartKey"] = last_key

            response = None
            for attempt in range(MAX_RETRIES):
                try:
                    response = table.scan(**kwargs)
                    break
                except Exception as e:
                    backoff = min(INITIAL_BACKOFF * (2 ** attempt), MAX_BACKOFF)
                    logger.warning(
                        f"[DDB] seg={segment} attempt {attempt + 1} failed: {e}"
                    )
                    time.sleep(backoff)
                    if attempt == MAX_RETRIES - 1:
                        raise RuntimeError(
                            f"[DDB] Segment {segment} exhausted {MAX_RETRIES} retries"
                        ) from e

            items = response.get("Items", [])
            lines = []
            for item in items:
                chunk_id = str(item.get("id", ""))
                doc_id = str(item.get("doc_id", ""))
                if chunk_id and doc_id:
                    lines.append(f"{chunk_id}\t{doc_id}\n")
                    count += 1

            if lines:
                with file_lock:
                    fh.writelines(lines)

            pbar.update(len(items))
            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                break

        return count

    def _scan_ddb_to_file(self):
        """Parallel-segment DDB scan (synchronous, runs in thread)."""
        completed, total_count = self._load_ddb_checkpoint()
        remaining = [s for s in range(DDB_TOTAL_SEGMENTS) if s not in completed]

        if not remaining:
            tqdm.write(f"[DDB] Already complete ({total_count:,} chunks)")
            return total_count

        tqdm.write(
            f"[DDB] Scanning {len(remaining)} of {DDB_TOTAL_SEGMENTS} segments "
            f"with {DDB_SCAN_WORKERS} workers..."
        )
        file_mode = "a" if completed else "w"
        file_lock = threading.Lock()
        cp_lock = threading.Lock()
        segs_since_save = 0

        pbar = tqdm(
            desc="[DDB] Scanning",
            unit=" items",
            position=0,
            leave=True,
            initial=total_count,
        )

        with open(DDB_RAW_FILE, file_mode, buffering=8 * 1024 * 1024) as fh:
            with ThreadPoolExecutor(max_workers=DDB_SCAN_WORKERS) as pool:
                futures = {
                    pool.submit(
                        self._scan_ddb_segment,
                        seg, DDB_TOTAL_SEGMENTS, file_lock, fh, pbar,
                    ): seg
                    for seg in remaining
                }
                for future in as_completed(futures):
                    seg = futures[future]
                    try:
                        seg_count = future.result()
                        with cp_lock:
                            total_count += seg_count
                            completed.add(seg)
                            segs_since_save += 1
                            if segs_since_save >= 10:
                                self._save_ddb_checkpoint(completed, total_count)
                                segs_since_save = 0
                    except Exception as e:
                        logger.error(f"[DDB] Segment {seg} failed: {e}")
                        self._save_ddb_checkpoint(completed, total_count)
                        raise RuntimeError(
                            f"[DDB] Segment {seg} failed. "
                            f"Checkpoint saved ({len(completed)}/{DDB_TOTAL_SEGMENTS}). "
                            f"Re-run to resume."
                        ) from e

        pbar.close()
        DDB_CHECKPOINT.unlink(missing_ok=True)
        logger.info(f"[DDB] Complete: {total_count:,} chunks -> {DDB_RAW_FILE}")
        return total_count

    async def _scan_ddb_async(self):
        return await asyncio.to_thread(self._scan_ddb_to_file)

    # =====================================================================
    # PHASE: scan — Qdrant scroll
    # =====================================================================
    async def _create_qdrant_client(self):
        return AsyncQdrantClient(
            url=self.qdrant_url,
            api_key=self.qdrant_api_key,
            timeout=60,
            prefer_grpc=True,
        )

    def _load_qdrant_checkpoint(self):
        if QDRANT_CHECKPOINT.exists():
            try:
                data = json.loads(QDRANT_CHECKPOINT.read_text())
                offset = data.get("offset")
                count = data.get("count", 0)
                tqdm.write(f"[Qdrant] Resuming from checkpoint at {count:,} points")
                return offset, count
            except Exception as e:
                logger.warning(f"[Qdrant] Bad checkpoint: {e}")
        return None, 0

    def _save_qdrant_checkpoint(self, offset, count):
        try:
            QDRANT_CHECKPOINT.write_text(json.dumps({
                "offset": str(offset) if offset else None,
                "count": count,
                "timestamp": datetime.utcnow().isoformat(),
            }))
        except Exception as e:
            logger.warning(f"[Qdrant] Checkpoint save failed: {e}")

    async def _scan_qdrant_to_file(self):
        """Async Qdrant scroll — writes point_id, doc_id, data_source_id to disk."""
        offset, count = self._load_qdrant_checkpoint()
        file_mode = "a" if count > 0 and QDRANT_RAW_FILE.exists() else "w"
        client = None
        batches_since_save = 0

        try:
            client = await self._create_qdrant_client()
            pbar = tqdm(
                desc="[Qdrant] Scrolling",
                unit=" points",
                position=1,
                leave=True,
                initial=count,
            )

            with open(QDRANT_RAW_FILE, file_mode, buffering=8 * 1024 * 1024) as fh:
                while True:
                    response = None
                    for attempt in range(MAX_RETRIES):
                        try:
                            response = await client.scroll(
                                collection_name=self.qdrant_collection,
                                limit=QDRANT_SCROLL_BATCH,
                                offset=offset,
                                with_payload=True,
                                with_vectors=False,
                            )
                            break
                        except Exception as e:
                            backoff = min(INITIAL_BACKOFF * (2 ** attempt), MAX_BACKOFF)
                            logger.warning(
                                f"[Qdrant] Attempt {attempt + 1} failed: {e}"
                            )
                            tqdm.write(
                                f"[Qdrant] Retry {attempt + 1}/{MAX_RETRIES} "
                                f"in {backoff}s — {type(e).__name__}"
                            )
                            await asyncio.sleep(backoff)
                            # Reconnect
                            try:
                                if client:
                                    await client.close()
                            except Exception:
                                pass
                            try:
                                client = await self._create_qdrant_client()
                            except Exception as re_err:
                                logger.warning(f"[Qdrant] Reconnect failed: {re_err}")
                            if attempt == MAX_RETRIES - 1:
                                self._save_qdrant_checkpoint(offset, count)
                                raise RuntimeError(
                                    f"[Qdrant] Failed after {MAX_RETRIES} retries. "
                                    f"Checkpoint saved at {count:,} points."
                                ) from e

                    if response is None:
                        break
                    points, next_offset = response
                    if not points:
                        break

                    for pt in points:
                        payload = pt.payload or {}
                        doc_id = payload.get("doc_id", "")
                        dsid = payload.get("data_source_id", "")
                        fh.write(f"{pt.id}\t{doc_id}\t{dsid}\n")
                        count += 1

                    pbar.update(len(points))

                    if next_offset is None:
                        break
                    offset = next_offset
                    batches_since_save += 1
                    if batches_since_save >= CHECKPOINT_INTERVAL:
                        fh.flush()
                        self._save_qdrant_checkpoint(offset, count)
                        batches_since_save = 0

            pbar.close()
            if client:
                await client.close()
            QDRANT_CHECKPOINT.unlink(missing_ok=True)
            logger.info(f"[Qdrant] Complete: {count:,} points -> {QDRANT_RAW_FILE}")
            return count

        except Exception as e:
            logger.error(f"[Qdrant] Fatal: {e}", exc_info=True)
            tqdm.write(f"[Qdrant] Error: {e}")
            if client:
                try:
                    await client.close()
                except Exception:
                    pass
            raise

    # =====================================================================
    # PHASE: scan (entry point)
    # =====================================================================
    async def phase_scan(self):
        print(f"\n{'=' * 60}")
        print("Phase: scan — Concurrent DDB + Qdrant scan to disk")
        print(f"{'=' * 60}\n")

        results = await asyncio.gather(
            self._scan_ddb_async(),
            self._scan_qdrant_to_file(),
            return_exceptions=True,
        )
        print("\n")

        ddb_result, qdrant_result = results
        failures = []

        if isinstance(ddb_result, Exception):
            logger.error(f"[DDB] Scan failed: {ddb_result}")
            print(f"[DDB] FAILED: {ddb_result}")
            failures.append(str(ddb_result))
        else:
            print(f"[DDB] Complete: {ddb_result:,} chunks")

        if isinstance(qdrant_result, Exception):
            logger.error(f"[Qdrant] Scan failed: {qdrant_result}")
            print(f"[Qdrant] FAILED: {qdrant_result}")
            failures.append(str(qdrant_result))
        else:
            print(f"[Qdrant] Complete: {qdrant_result:,} points")

        if failures:
            print(
                "\nOne or both scans failed. Checkpoints saved. "
                "Re-run --phase scan to resume."
            )
            sys.exit(1)

        print("\nScan complete. Run --phase diff next.")

    # =====================================================================
    # PHASE: diff
    # =====================================================================
    def phase_diff(self):
        for f, name in [(DDB_RAW_FILE, "ddb_raw.tsv"), (QDRANT_RAW_FILE, "qdrant_raw.tsv")]:
            if not f.exists():
                print(f"Error: {name} not found. Run --phase scan first.")
                sys.exit(1)

        print(f"\n{'=' * 60}")
        print("Phase: diff — Sort and compute differences")
        print(f"{'=' * 60}\n")

        # 1. Extract ID columns
        print("Extracting IDs...")
        self._run_pipe(["cut", "-f1", str(DDB_RAW_FILE)], DDB_IDS_RAW)
        self._run_pipe(["cut", "-f1", str(QDRANT_RAW_FILE)], QDRANT_IDS_RAW)

        # 2. Sort + dedup
        ddb_count = self._sort_file(DDB_IDS_RAW, DDB_IDS_SORTED, "DDB IDs")
        qdrant_count = self._sort_file(QDRANT_IDS_RAW, QDRANT_IDS_SORTED, "Qdrant IDs")

        # 3. Set differences
        print("\nComputing set differences...")
        missing_count = self._comm_diff(DDB_IDS_SORTED, QDRANT_IDS_SORTED, MISSING_FILE)
        orphan_count = self._comm_diff(QDRANT_IDS_SORTED, DDB_IDS_SORTED, ORPHANS_FILE)

        print(f"  Missing in Qdrant (need embed): {missing_count:,}")
        print(f"  Orphans in Qdrant (need delete): {orphan_count:,}")

        # 4. Build enrichment files for --phase embed
        if missing_count > 0:
            print("\nBuilding enrichment files for --phase embed...")

            # Sort ddb_raw.tsv by chunk_id for merge-join
            self._sort_file(
                DDB_RAW_FILE, DDB_RAW_BY_ID, "DDB by chunk_id",
                unique=True, tab_key="1,1", delete_input=False,
            )

            # Join: missing chunk_ids ⋈ ddb metadata → chunk_id\tdoc_id
            self._run_pipe(
                ["join", "-t\t", str(MISSING_FILE), str(DDB_RAW_BY_ID)],
                MISSING_ENRICHED,
            )

            # Swap columns (chunk_id\tdoc_id → doc_id\tchunk_id) for doc grouping
            with open(MISSING_ENRICHED) as fin, \
                    open(MISSING_BY_DOC_UNSORTED, "w", buffering=8 * 1024 * 1024) as fout:
                for line in fin:
                    parts = line.rstrip("\n").split("\t", 1)
                    if len(parts) == 2:
                        fout.write(f"{parts[1]}\t{parts[0]}\n")

            # Sort by doc_id for streaming
            self._sort_file(
                MISSING_BY_DOC_UNSORTED, MISSING_BY_DOC, "missing by doc_id",
                unique=False, tab_key="1,1",
            )

            # Build doc_id → data_source_id map from Qdrant payload data
            self._run_pipe(["cut", "-f2,3", str(QDRANT_RAW_FILE)], DOC_DSID_RAW)
            self._sort_file(
                DOC_DSID_RAW, DOC_DSID_MAP, "doc→dsid map",
                unique=True, tab_key="1,1",
            )

            # Cleanup intermediates
            for tmp in [DDB_RAW_BY_ID, MISSING_ENRICHED]:
                tmp.unlink(missing_ok=True)

        # Summary
        print(f"\n{'=' * 60}")
        print("Diff Results:")
        print(f"  Total unique in DDB:    {ddb_count:,}")
        print(f"  Total unique in Qdrant: {qdrant_count:,}")
        print(f"  Missing in Qdrant:      {missing_count:,}")
        print(f"  Orphans in Qdrant:      {orphan_count:,}")
        print(f"{'=' * 60}")

        if missing_count > 0:
            print("\nRun --phase embed next to fix missing chunks.")
        if orphan_count > 0:
            print("Run --phase delete to remove orphan points.")
        if missing_count == 0 and orphan_count == 0:
            print("\nDDB and Qdrant are fully in sync!")

    # =====================================================================
    # PHASE: embed — helpers
    # =====================================================================
    @staticmethod
    def _load_doc_dsid_map():
        """Load doc_id → data_source_id mapping from disk into dict."""
        mapping = {}
        if DOC_DSID_MAP.exists():
            with open(DOC_DSID_MAP) as f:
                for line in f:
                    parts = line.rstrip("\n").split("\t", 1)
                    if len(parts) == 2 and parts[0] and parts[1]:
                        mapping[parts[0]] = parts[1]
        logger.info(f"Loaded doc→dsid map: {len(mapping):,} entries")
        return mapping

    async def _resolve_dsid_supabase(self, doc_ids, http_client):
        """Batch lookup data_source_id from Supabase documents table."""
        results = {}
        for i in range(0, len(doc_ids), SUPABASE_BATCH_SIZE):
            batch = doc_ids[i : i + SUPABASE_BATCH_SIZE]
            ids_csv = ",".join(batch)
            url = (
                f"{self.supabase_url}/rest/v1/documents"
                f"?select=id,data_source_id&id=in.({ids_csv})"
            )
            headers = {
                "apikey": self.supabase_key,
                "Authorization": f"Bearer {self.supabase_key}",
            }
            for attempt in range(3):
                try:
                    resp = await http_client.get(url, headers=headers, timeout=30)
                    resp.raise_for_status()
                    for row in resp.json():
                        dsid = row.get("data_source_id")
                        if dsid:
                            results[row["id"]] = str(dsid)
                    break
                except Exception as e:
                    if attempt == 2:
                        logger.warning(f"Supabase batch lookup failed: {e}")
                    else:
                        await asyncio.sleep(2 ** attempt)
        return results

    async def _embed_batch(self, texts, voyage_client, semaphore):
        """Embed texts with Voyage AI, with retry and auto-split on token limit."""
        async with semaphore:
            for attempt in range(EMBED_MAX_RETRIES):
                try:
                    result = await voyage_client.embed(
                        texts=texts, model="voyage-3", input_type="document",
                    )
                    return result.embeddings
                except Exception as e:
                    err_lower = str(e).lower()
                    # Token limit → split in half and retry each half
                    if "token" in err_lower and len(texts) > 1:
                        mid = len(texts) // 2
                        left = await self._embed_batch(
                            texts[:mid], voyage_client, semaphore,
                        )
                        right = await self._embed_batch(
                            texts[mid:], voyage_client, semaphore,
                        )
                        return left + right
                    backoff = min(INITIAL_BACKOFF * (2 ** attempt), 120)
                    logger.warning(
                        f"Voyage attempt {attempt + 1}/{EMBED_MAX_RETRIES} "
                        f"failed: {e}"
                    )
                    if attempt < EMBED_MAX_RETRIES - 1:
                        await asyncio.sleep(backoff)
                    else:
                        raise

    async def _qdrant_upsert(self, client, points):
        """Upsert points to Qdrant with retry."""
        for attempt in range(QDRANT_UPSERT_RETRIES):
            try:
                await client.upsert(
                    collection_name=self.qdrant_collection,
                    points=points,
                    wait=True,
                )
                return
            except Exception as e:
                backoff = min(INITIAL_BACKOFF * (2 ** attempt), 60)
                logger.warning(f"Qdrant upsert attempt {attempt + 1} failed: {e}")
                if attempt < QDRANT_UPSERT_RETRIES - 1:
                    await asyncio.sleep(backoff)
                else:
                    raise

    @staticmethod
    def _stream_missing_by_doc():
        """Yield (doc_id, [chunk_ids]) groups from sorted file."""
        current_doc = None
        current_chunks = []
        with open(MISSING_BY_DOC) as f:
            for line in f:
                parts = line.rstrip("\n").split("\t", 1)
                if len(parts) != 2:
                    continue
                doc_id, chunk_id = parts
                if doc_id != current_doc:
                    if current_doc is not None:
                        yield current_doc, current_chunks
                    current_doc = doc_id
                    current_chunks = [chunk_id]
                else:
                    current_chunks.append(chunk_id)
        if current_doc is not None:
            yield current_doc, current_chunks

    def _fetch_ddb_chunks(self, doc_id, table):
        """Fetch all chunks for a doc_id from DDB (sync)."""
        chunks = []
        last_key = None
        while True:
            kwargs = {"KeyConditionExpression": Key("doc_id").eq(doc_id)}
            if last_key:
                kwargs["ExclusiveStartKey"] = last_key

            for attempt in range(MAX_RETRIES):
                try:
                    response = table.query(**kwargs)
                    break
                except Exception as e:
                    backoff = min(INITIAL_BACKOFF * (2 ** attempt), MAX_BACKOFF)
                    logger.warning(
                        f"DDB query doc_id={doc_id} attempt {attempt + 1}: {e}"
                    )
                    time.sleep(backoff)
                    if attempt == MAX_RETRIES - 1:
                        raise

            for item in response.get("Items", []):
                chunk_id = str(item.get("id", ""))
                content = str(item.get("content", ""))
                if chunk_id:
                    chunks.append({"id": chunk_id, "content": content})

            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                break
        return chunks

    @staticmethod
    def _load_embed_checkpoint():
        if EMBED_CHECKPOINT.exists():
            try:
                data = json.loads(EMBED_CHECKPOINT.read_text())
                return data.get("last_doc_id"), data.get("embedded_count", 0)
            except Exception:
                pass
        return None, 0

    @staticmethod
    def _save_embed_checkpoint(last_doc_id, embedded_count):
        EMBED_CHECKPOINT.write_text(json.dumps({
            "last_doc_id": last_doc_id,
            "embedded_count": embedded_count,
            "timestamp": datetime.utcnow().isoformat(),
        }))

    # =====================================================================
    # PHASE: embed (entry point)
    # =====================================================================
    async def phase_embed(self):
        # --- Prerequisite checks ---
        if not MISSING_FILE.exists():
            print("Error: missing_in_qdrant.txt not found. Run --phase diff first.")
            sys.exit(1)
        if not MISSING_BY_DOC.exists():
            cnt = self._count_lines(MISSING_FILE)
            if cnt == 0:
                print("No missing chunks to embed. Nothing to do.")
                return
            print(
                f"Error: missing_by_doc.tsv not found but {cnt:,} missing chunks exist. "
                "Re-run --phase diff."
            )
            sys.exit(1)

        missing_count = self._count_lines(MISSING_BY_DOC)
        if missing_count == 0:
            print("No missing chunks to embed. Nothing to do.")
            return

        # Count unique docs
        unique_docs = set()
        with open(MISSING_BY_DOC) as f:
            for line in f:
                d = line.split("\t", 1)[0]
                if d:
                    unique_docs.add(d)

        est_batches = (missing_count + EMBED_BATCH_SIZE - 1) // EMBED_BATCH_SIZE

        last_doc_id, already_embedded = self._load_embed_checkpoint()
        remaining = missing_count - already_embedded if last_doc_id else missing_count

        # --- Summary ---
        print(f"\n{'=' * 60}")
        print("=== Embed & Upsert Summary ===")
        print(f"  Missing chunks:            {missing_count:,}")
        print(f"  Unique documents:          {len(unique_docs):,}")
        print(f"  Est. Voyage batches:       ~{est_batches:,}")
        if last_doc_id:
            print(f"  Already embedded:          {already_embedded:,}")
            print(f"  Remaining:                 ~{remaining:,}")
        print(f"{'=' * 60}")

        confirm = input("\nProceed? [y/N]: ").strip().lower()
        if confirm != "y":
            print("Aborted.")
            return

        # --- Init clients ---
        dsid_map = self._load_doc_dsid_map()
        print(f"  Loaded {len(dsid_map):,} doc→dsid mappings from Qdrant data")

        voyage_client = voyageai.AsyncClient(api_key=self.voyage_api_key)
        qdrant_client = await self._create_qdrant_client()
        http_client = httpx.AsyncClient()
        semaphore = asyncio.Semaphore(EMBED_CONCURRENCY)

        ddb_config = Config(
            region_name=self.ddb_region,
            retries={"max_attempts": 10, "mode": "adaptive"},
        )
        ddb_table = boto3.resource("dynamodb", config=ddb_config).Table(
            self.ddb_table_name
        )

        # --- Process ---
        embedded_count = already_embedded
        skipped_no_dsid = 0
        skipped_no_content = 0
        embed_buffer = []  # [{chunk_id, content, doc_id, data_source_id}]

        pbar = tqdm(
            desc="Embedding", unit=" chunks",
            total=missing_count, initial=already_embedded,
        )

        async def _flush_buffer(buf):
            """Embed + upsert a buffer of chunks."""
            if not buf:
                return
            texts = [item["content"] for item in buf]
            embeddings = await self._embed_batch(texts, voyage_client, semaphore)
            points = [
                PointStruct(
                    id=item["chunk_id"],
                    vector=emb,
                    payload={
                        "doc_id": item["doc_id"],
                        "data_source_id": item["data_source_id"],
                    },
                )
                for item, emb in zip(buf, embeddings)
            ]
            for j in range(0, len(points), QDRANT_UPSERT_BATCH):
                await self._qdrant_upsert(
                    qdrant_client, points[j : j + QDRANT_UPSERT_BATCH]
                )

        try:
            for doc_id, chunk_ids in self._stream_missing_by_doc():
                # Resume: skip already-processed docs
                if last_doc_id and doc_id <= last_doc_id:
                    continue

                chunk_id_set = set(chunk_ids)

                # Resolve data_source_id
                data_source_id = dsid_map.get(doc_id)
                if not data_source_id:
                    sb = await self._resolve_dsid_supabase([doc_id], http_client)
                    data_source_id = sb.get(doc_id)
                if not data_source_id:
                    skipped_no_dsid += len(chunk_ids)
                    pbar.update(len(chunk_ids))
                    logger.warning(
                        f"No data_source_id for doc_id={doc_id}, "
                        f"skipping {len(chunk_ids)} chunks"
                    )
                    continue

                # Fetch chunk content from DDB
                ddb_chunks = await asyncio.to_thread(
                    self._fetch_ddb_chunks, doc_id, ddb_table,
                )

                added_this_doc = 0
                for chunk in ddb_chunks:
                    if chunk["id"] not in chunk_id_set:
                        continue
                    if not chunk["content"].strip():
                        skipped_no_content += 1
                        pbar.update(1)
                        continue
                    embed_buffer.append({
                        "chunk_id": chunk["id"],
                        "content": chunk["content"],
                        "doc_id": doc_id,
                        "data_source_id": data_source_id,
                    })
                    added_this_doc += 1

                # Flush full batches
                while len(embed_buffer) >= EMBED_BATCH_SIZE:
                    batch = embed_buffer[:EMBED_BATCH_SIZE]
                    embed_buffer = embed_buffer[EMBED_BATCH_SIZE:]
                    await _flush_buffer(batch)
                    embedded_count += len(batch)
                    pbar.update(len(batch))

                self._save_embed_checkpoint(doc_id, embedded_count)

            # Flush remaining
            if embed_buffer:
                await _flush_buffer(embed_buffer)
                embedded_count += len(embed_buffer)
                pbar.update(len(embed_buffer))
                embed_buffer = []

        finally:
            pbar.close()
            await qdrant_client.close()
            await http_client.aclose()

        EMBED_CHECKPOINT.unlink(missing_ok=True)

        print(f"\n{'=' * 60}")
        print("Embed & Upsert Complete:")
        print(f"  Embedded + upserted:  {embedded_count:,}")
        print(f"  Skipped (no dsid):    {skipped_no_dsid:,}")
        print(f"  Skipped (no content): {skipped_no_content:,}")
        print(f"{'=' * 60}")

    # =====================================================================
    # PHASE: delete
    # =====================================================================
    @staticmethod
    def _load_delete_checkpoint():
        if DELETE_CHECKPOINT.exists():
            try:
                data = json.loads(DELETE_CHECKPOINT.read_text())
                return data.get("deleted_count", 0), data.get("line_offset", 0)
            except Exception:
                pass
        return 0, 0

    @staticmethod
    def _save_delete_checkpoint(deleted_count, line_offset):
        DELETE_CHECKPOINT.write_text(json.dumps({
            "deleted_count": deleted_count,
            "line_offset": line_offset,
            "timestamp": datetime.utcnow().isoformat(),
        }))

    async def phase_delete(self):
        if not ORPHANS_FILE.exists():
            print("Error: orphans_in_qdrant.txt not found. Run --phase diff first.")
            sys.exit(1)

        orphan_count = self._count_lines(ORPHANS_FILE)
        if orphan_count == 0:
            print("No orphan points to delete. Nothing to do.")
            return

        deleted_so_far, line_offset = self._load_delete_checkpoint()
        remaining = orphan_count - line_offset
        est_batches = (remaining + QDRANT_DELETE_BATCH - 1) // QDRANT_DELETE_BATCH

        print(f"\n{'=' * 60}")
        print("=== Delete Orphans Summary ===")
        print(f"  Orphan points:            {orphan_count:,}")
        print(f"  Est. delete batches:      ~{est_batches:,}")
        if line_offset > 0:
            print(f"  Already deleted:          {deleted_so_far:,}")
            print(f"  Remaining:                {remaining:,}")
        print(f"{'=' * 60}")

        confirm = input("\nProceed? [y/N]: ").strip().lower()
        if confirm != "y":
            print("Aborted.")
            return

        client = await self._create_qdrant_client()
        pbar = tqdm(
            desc="Deleting", unit=" points",
            total=orphan_count, initial=line_offset,
        )

        try:
            batch = []
            current_line = 0

            with open(ORPHANS_FILE) as f:
                for line in f:
                    current_line += 1
                    if current_line <= line_offset:
                        continue

                    point_id = line.strip()
                    if not point_id:
                        continue
                    batch.append(point_id)

                    if len(batch) >= QDRANT_DELETE_BATCH:
                        await self._delete_batch(client, batch)
                        deleted_so_far += len(batch)
                        pbar.update(len(batch))
                        self._save_delete_checkpoint(deleted_so_far, current_line)
                        batch = []

            # Flush remaining
            if batch:
                await self._delete_batch(client, batch)
                deleted_so_far += len(batch)
                pbar.update(len(batch))

        finally:
            pbar.close()
            await client.close()

        DELETE_CHECKPOINT.unlink(missing_ok=True)

        print(f"\n{'=' * 60}")
        print("Delete Complete:")
        print(f"  Deleted: {deleted_so_far:,} orphan points")
        print(f"{'=' * 60}")

    async def _delete_batch(self, client, batch):
        """Delete a batch of point IDs with retry."""
        for attempt in range(QDRANT_DELETE_RETRIES):
            try:
                await client.delete(
                    collection_name=self.qdrant_collection,
                    points_selector=PointIdsList(points=batch),
                )
                return
            except Exception as e:
                backoff = min(INITIAL_BACKOFF * (2 ** attempt), 60)
                logger.warning(f"Delete attempt {attempt + 1} failed: {e}")
                if attempt < QDRANT_DELETE_RETRIES - 1:
                    await asyncio.sleep(backoff)
                else:
                    raise


# ===========================================================================
# CLI entry point
# ===========================================================================
async def main():
    parser = argparse.ArgumentParser(
        description="Sync DynamoDB chunks ↔ Qdrant vectors",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Phases (run sequentially):
  scan    Concurrently scan DDB + Qdrant to disk files
  diff    Sort and compute missing/orphan sets
  embed   Embed missing chunks and upsert to Qdrant (confirms first)
  delete  Delete orphan Qdrant points (confirms first)
        """,
    )
    parser.add_argument(
        "--phase", required=True,
        choices=["scan", "diff", "embed", "delete"],
        help="Which phase to run",
    )
    args = parser.parse_args()

    WORK_DIR.mkdir(parents=True, exist_ok=True)
    syncer = ChunkSyncer()

    if args.phase == "scan":
        await syncer.phase_scan()
    elif args.phase == "diff":
        syncer.phase_diff()
    elif args.phase == "embed":
        await syncer.phase_embed()
    elif args.phase == "delete":
        await syncer.phase_delete()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted. Checkpoints saved where applicable.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal: {e}", exc_info=True)
        print(f"\nFatal error: {e}")
        sys.exit(1)
