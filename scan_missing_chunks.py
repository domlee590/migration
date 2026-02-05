#!/usr/bin/env python3
"""
Scan Missing Chunks Script (Memory-Efficient Version)

Scans both Weaviate and Qdrant in parallel to identify chunks that exist in
Weaviate but are missing from Qdrant. Uses disk-based sorting to avoid OOM.

Features:
    - Aggressive retry with exponential backoff (up to 5 min between retries)
    - Client reconnection on failure
    - Checkpoint/resume: scans save progress to disk and resume from last offset
    - Independent failure handling: one database failing doesn't kill the other

Writes results to missing_chunk_ids.json.

Usage:
    python scan_missing_chunks.py
"""

import os
import sys
import json
import time
import asyncio
import logging
import subprocess
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from tqdm import tqdm

import weaviate
from qdrant_client import AsyncQdrantClient

# Configuration
ENV_FILE = Path(__file__).parent / ".env"
OUTPUT_FILE = Path(__file__).parent / "missing_chunk_ids.json"
LOG_FILE = Path(__file__).parent / "scan_missing_chunks.log"

# Temporary working files for disk-based processing
WORK_DIR = Path(__file__).parent
WEAVIATE_RAW_FILE = WORK_DIR / "weaviate_ids_raw.txt"
QDRANT_RAW_FILE = WORK_DIR / "qdrant_ids_raw.txt"
WEAVIATE_SORTED_FILE = WORK_DIR / "weaviate_ids_sorted.txt"
QDRANT_SORTED_FILE = WORK_DIR / "qdrant_ids_sorted.txt"
MISSING_IDS_FILE = WORK_DIR / "missing_ids.txt"

# Checkpoint files for resume support
WEAVIATE_CHECKPOINT = WORK_DIR / "weaviate_scan_checkpoint.json"
QDRANT_CHECKPOINT = WORK_DIR / "qdrant_scan_checkpoint.json"

# Retry configuration
MAX_RETRIES = 10              # retries per batch
INITIAL_BACKOFF = 2           # seconds
MAX_BACKOFF = 300             # 5 minutes max between retries
CHECKPOINT_INTERVAL = 50      # save checkpoint every N batches

# Setup logging - only log to file to keep console clean for tqdm
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE)],
)
logger = logging.getLogger(__name__)

# Suppress noisy HTTP logs from various libraries
for noisy_logger in ["httpx", "httpcore", "urllib3", "weaviate", "qdrant_client"]:
    logging.getLogger(noisy_logger).setLevel(logging.WARNING)


class ChunkScanner:
    """Scans Weaviate and Qdrant to identify missing chunks using disk-based processing."""

    def __init__(self):
        """Initialize scanner and load configuration."""
        self.load_environment()

    def load_environment(self):
        """Load configuration from .env file."""
        if not ENV_FILE.exists():
            raise FileNotFoundError(f"Environment file not found: {ENV_FILE}")

        load_dotenv(ENV_FILE)

        # Weaviate config
        self.weaviate_url = os.getenv("WEAVIATE_URL")
        self.weaviate_api_key = os.getenv("WEAVIATE_API_KEY")
        self.weaviate_collection = os.getenv("WEAVIATE_COLLECTION", "Text_tables")

        # Qdrant config
        self.qdrant_url = os.getenv("QDRANT_URL")
        self.qdrant_api_key = os.getenv("QDRANT_API_KEY")
        self.qdrant_collection = os.getenv("QDRANT_COLLECTION", "chunks")

        # Validate required vars
        required = [
            ("WEAVIATE_URL", self.weaviate_url),
            ("WEAVIATE_API_KEY", self.weaviate_api_key),
            ("QDRANT_URL", self.qdrant_url),
            ("QDRANT_API_KEY", self.qdrant_api_key),
        ]

        missing = [name for name, value in required if not value]
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )

        logger.info("Environment configuration loaded successfully")
        logger.info(f"Weaviate: {self.weaviate_url} / {self.weaviate_collection}")
        logger.info(f"Qdrant: {self.qdrant_url} / {self.qdrant_collection}")
        print(f"Weaviate: {self.weaviate_url} / {self.weaviate_collection}")
        print(f"Qdrant: {self.qdrant_url} / {self.qdrant_collection}")

    def _create_weaviate_client(self):
        """Create a fresh Weaviate client connection."""
        return weaviate.connect_to_weaviate_cloud(
            cluster_url=self.weaviate_url,
            auth_credentials=weaviate.classes.init.Auth.api_key(
                self.weaviate_api_key
            ),
            skip_init_checks=False,
        )

    def _load_weaviate_checkpoint(self):
        """Load Weaviate checkpoint from disk if it exists.

        Returns:
            (cursor, count) or (None, 0) if no checkpoint
        """
        if WEAVIATE_CHECKPOINT.exists():
            try:
                data = json.loads(WEAVIATE_CHECKPOINT.read_text())
                cursor = data.get("cursor")
                count = data.get("count", 0)
                logger.info(f"[Weaviate] Resuming from checkpoint: cursor={cursor}, count={count:,}")
                tqdm.write(f"[Weaviate] Resuming from checkpoint at {count:,} chunks")
                return cursor, count
            except Exception as e:
                logger.warning(f"[Weaviate] Failed to load checkpoint, starting fresh: {e}")
        return None, 0

    def _save_weaviate_checkpoint(self, cursor, count):
        """Save Weaviate progress to checkpoint file."""
        try:
            WEAVIATE_CHECKPOINT.write_text(json.dumps({
                "cursor": str(cursor) if cursor else None,
                "count": count,
                "timestamp": datetime.utcnow().isoformat(),
            }))
        except Exception as e:
            logger.warning(f"[Weaviate] Failed to save checkpoint: {e}")

    def _scan_weaviate_to_file(self) -> int:
        """Synchronous Weaviate scan - writes chunk_ids directly to disk.

        Features:
            - Resumes from checkpoint if previous run was interrupted
            - Reconnects client on failure
            - Exponential backoff up to 5 minutes between retries

        Returns:
            Count of chunk_ids written
        """
        logger.info(f"[Weaviate] Starting scan of {self.weaviate_collection}...")

        batch_size = 10000
        client = None
        batches_since_checkpoint = 0

        # Load checkpoint for resume
        cursor, count = self._load_weaviate_checkpoint()
        file_mode = "a" if count > 0 and WEAVIATE_RAW_FILE.exists() else "w"

        try:
            client = self._create_weaviate_client()
            collection = client.collections.get(self.weaviate_collection)

            pbar = tqdm(
                desc="[Weaviate] Scanning chunk_ids",
                unit=" chunks",
                position=0,
                leave=True,
                initial=count,
            )

            with open(WEAVIATE_RAW_FILE, file_mode, buffering=8 * 1024 * 1024) as f:
                while True:
                    response = None

                    for attempt in range(MAX_RETRIES):
                        try:
                            response = collection.query.fetch_objects(
                                limit=batch_size,
                                after=cursor,
                                return_properties=["chunk_id"],
                            )
                            break
                        except Exception as e:
                            backoff = min(INITIAL_BACKOFF * (2 ** attempt), MAX_BACKOFF)
                            logger.warning(
                                f"[Weaviate] Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}. "
                                f"Retrying in {backoff}s..."
                            )
                            tqdm.write(
                                f"[Weaviate] Retry {attempt + 1}/{MAX_RETRIES} in {backoff}s - {type(e).__name__}: {e}"
                            )
                            time.sleep(backoff)

                            # Reconnect client on failure
                            try:
                                if client:
                                    client.close()
                            except Exception:
                                pass
                            try:
                                client = self._create_weaviate_client()
                                collection = client.collections.get(self.weaviate_collection)
                                logger.info("[Weaviate] Reconnected client successfully")
                            except Exception as reconnect_err:
                                logger.warning(f"[Weaviate] Reconnect failed: {reconnect_err}")

                            if attempt == MAX_RETRIES - 1:
                                # Save checkpoint before giving up so we can resume later
                                self._save_weaviate_checkpoint(cursor, count)
                                raise RuntimeError(
                                    f"[Weaviate] Failed after {MAX_RETRIES} retries. "
                                    f"Checkpoint saved at {count:,} chunks. "
                                    f"Re-run to resume. Last error: {e}"
                                ) from e

                    if not response or not response.objects:
                        break

                    for obj in response.objects:
                        chunk_id = obj.properties.get("chunk_id")
                        if chunk_id:
                            f.write(str(chunk_id) + "\n")
                            count += 1

                    pbar.update(len(response.objects))

                    if len(response.objects) < batch_size:
                        break

                    cursor = response.objects[-1].uuid
                    batches_since_checkpoint += 1

                    # Periodic checkpoint
                    if batches_since_checkpoint >= CHECKPOINT_INTERVAL:
                        f.flush()
                        self._save_weaviate_checkpoint(cursor, count)
                        batches_since_checkpoint = 0

            pbar.close()

            if client:
                client.close()

            # Clean up checkpoint on successful completion
            WEAVIATE_CHECKPOINT.unlink(missing_ok=True)

            logger.info(
                f"[Weaviate] Complete: {count:,} chunk_ids written to {WEAVIATE_RAW_FILE}"
            )
            return count

        except Exception as e:
            logger.error(f"[Weaviate] Failed to scan: {e}", exc_info=True)
            tqdm.write(f"[Weaviate] Error: {e}")
            if client:
                try:
                    client.close()
                except Exception:
                    pass
            raise

    async def scan_weaviate_to_file(self) -> int:
        """Scan Weaviate chunk_ids to disk file.

        Runs synchronous Weaviate client in thread executor to not block event loop.

        Returns:
            Count of chunk_ids written
        """
        return await asyncio.to_thread(self._scan_weaviate_to_file)

    async def _create_qdrant_client(self):
        """Create a fresh Qdrant async client connection."""
        return AsyncQdrantClient(
            url=self.qdrant_url,
            api_key=self.qdrant_api_key,
            timeout=60,
        )

    def _load_qdrant_checkpoint(self):
        """Load Qdrant checkpoint from disk if it exists.

        Returns:
            (offset, count) or (None, 0) if no checkpoint
        """
        if QDRANT_CHECKPOINT.exists():
            try:
                data = json.loads(QDRANT_CHECKPOINT.read_text())
                offset = data.get("offset")
                count = data.get("count", 0)
                logger.info(f"[Qdrant] Resuming from checkpoint: offset={offset}, count={count:,}")
                tqdm.write(f"[Qdrant] Resuming from checkpoint at {count:,} points")
                return offset, count
            except Exception as e:
                logger.warning(f"[Qdrant] Failed to load checkpoint, starting fresh: {e}")
        return None, 0

    def _save_qdrant_checkpoint(self, offset, count):
        """Save Qdrant progress to checkpoint file."""
        try:
            QDRANT_CHECKPOINT.write_text(json.dumps({
                "offset": str(offset) if offset else None,
                "count": count,
                "timestamp": datetime.utcnow().isoformat(),
            }))
        except Exception as e:
            logger.warning(f"[Qdrant] Failed to save checkpoint: {e}")

    async def scan_qdrant_to_file(self) -> int:
        """Scan all point IDs from Qdrant collection directly to disk file.

        Features:
            - Resumes from checkpoint if previous run was interrupted
            - Reconnects client on failure
            - Exponential backoff up to 5 minutes between retries

        Returns:
            Count of point_ids written
        """
        logger.info(f"[Qdrant] Starting scan of {self.qdrant_collection}...")

        batch_size = 10000
        client = None
        batches_since_checkpoint = 0

        # Load checkpoint for resume
        offset, count = self._load_qdrant_checkpoint()
        file_mode = "a" if count > 0 and QDRANT_RAW_FILE.exists() else "w"

        try:
            client = await self._create_qdrant_client()

            pbar = tqdm(
                desc="[Qdrant] Scanning point IDs",
                unit=" points",
                position=1,
                leave=True,
                initial=count,
            )

            with open(QDRANT_RAW_FILE, file_mode, buffering=8 * 1024 * 1024) as f:
                while True:
                    response = None

                    for attempt in range(MAX_RETRIES):
                        try:
                            response = await client.scroll(
                                collection_name=self.qdrant_collection,
                                limit=batch_size,
                                offset=offset,
                                with_payload=False,
                                with_vectors=False,
                            )
                            break
                        except Exception as e:
                            backoff = min(INITIAL_BACKOFF * (2 ** attempt), MAX_BACKOFF)
                            logger.warning(
                                f"[Qdrant] Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}. "
                                f"Retrying in {backoff}s..."
                            )
                            tqdm.write(
                                f"[Qdrant] Retry {attempt + 1}/{MAX_RETRIES} in {backoff}s - {type(e).__name__}: {e}"
                            )
                            await asyncio.sleep(backoff)

                            # Reconnect client on failure
                            try:
                                if client:
                                    await client.close()
                            except Exception:
                                pass
                            try:
                                client = await self._create_qdrant_client()
                                logger.info("[Qdrant] Reconnected client successfully")
                            except Exception as reconnect_err:
                                logger.warning(f"[Qdrant] Reconnect failed: {reconnect_err}")

                            if attempt == MAX_RETRIES - 1:
                                # Save checkpoint before giving up so we can resume later
                                self._save_qdrant_checkpoint(offset, count)
                                raise RuntimeError(
                                    f"[Qdrant] Failed after {MAX_RETRIES} retries. "
                                    f"Checkpoint saved at {count:,} points. "
                                    f"Re-run to resume. Last error: {e}"
                                ) from e

                    if response is None:
                        break

                    points, next_offset = response

                    if not points:
                        break

                    for point in points:
                        f.write(str(point.id) + "\n")
                        count += 1

                    pbar.update(len(points))

                    if next_offset is None:
                        break

                    offset = next_offset
                    batches_since_checkpoint += 1

                    # Periodic checkpoint
                    if batches_since_checkpoint >= CHECKPOINT_INTERVAL:
                        f.flush()
                        self._save_qdrant_checkpoint(offset, count)
                        batches_since_checkpoint = 0

            pbar.close()

            if client:
                await client.close()

            # Clean up checkpoint on successful completion
            QDRANT_CHECKPOINT.unlink(missing_ok=True)

            logger.info(
                f"[Qdrant] Complete: {count:,} point IDs written to {QDRANT_RAW_FILE}"
            )
            return count

        except Exception as e:
            logger.error(f"[Qdrant] Failed to scan: {e}", exc_info=True)
            tqdm.write(f"[Qdrant] Error: {e}")
            if client:
                try:
                    await client.close()
                except Exception:
                    pass
            raise

    def _sort_file(self, input_file: Path, output_file: Path, label: str) -> int:
        """Sort and deduplicate a file using OS-level sort (external merge sort).

        Uses minimal memory regardless of file size. The OS sort command
        automatically spills to disk for large files.

        Returns:
            Count of unique lines in the sorted file
        """
        print(f"  [{label}] Sorting {input_file.name}...")
        logger.info(f"[{label}] Sorting {input_file} -> {output_file}")

        # sort -u: sort and deduplicate
        # -S 4G: use up to 4GB for sort buffer (plenty on t3.2xlarge w/ 32GB)
        # --parallel=8: use all 8 vCPUs for parallel merge sort
        # -T: use working directory for temp files
        result = subprocess.run(
            [
                "sort",
                "-u",
                "-S",
                "4G",
                "--parallel=8",
                "-T",
                str(WORK_DIR),
                "-o",
                str(output_file),
                str(input_file),
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise RuntimeError(f"sort failed for {label}: {result.stderr}")

        # Count unique lines
        wc_result = subprocess.run(
            ["wc", "-l", str(output_file)],
            capture_output=True,
            text=True,
        )
        line_count = int(wc_result.stdout.strip().split()[0])

        print(f"  [{label}] Sorted: {line_count:,} unique IDs")
        logger.info(f"[{label}] Sorted: {line_count:,} unique IDs in {output_file}")

        # Remove raw file to free disk space
        input_file.unlink(missing_ok=True)

        return line_count

    def _compute_difference(self) -> int:
        """Use comm to find IDs in Weaviate but not in Qdrant.

        comm -23 outputs lines that are unique to file 1 (weaviate).
        Both files must be sorted, which they are from the sort step.

        Returns:
            Count of missing chunk IDs
        """
        print("  Computing set difference with comm...")
        logger.info("Computing set difference using comm...")

        # comm -23: suppress lines unique to file2 and common lines
        # → outputs lines unique to file1 (weaviate IDs not in qdrant)
        with open(MISSING_IDS_FILE, "w") as out:
            result = subprocess.run(
                ["comm", "-23", str(WEAVIATE_SORTED_FILE), str(QDRANT_SORTED_FILE)],
                stdout=out,
                stderr=subprocess.PIPE,
                text=True,
            )

        if result.returncode != 0:
            raise RuntimeError(f"comm failed: {result.stderr}")

        # Count missing IDs
        wc_result = subprocess.run(
            ["wc", "-l", str(MISSING_IDS_FILE)],
            capture_output=True,
            text=True,
        )
        missing_count = int(wc_result.stdout.strip().split()[0])

        logger.info(f"Missing chunk IDs: {missing_count:,}")
        return missing_count

    def _write_json_output(self, weaviate_count: int, qdrant_count: int, missing_count: int):
        """Write JSON output by streaming missing IDs from disk.

        Avoids loading all missing IDs into memory at once.
        """
        logger.info(f"Writing JSON output to {OUTPUT_FILE}...")

        with open(OUTPUT_FILE, "w") as f:
            f.write("{\n")
            f.write(f'  "scan_timestamp": "{datetime.utcnow().isoformat()}",\n')
            f.write(f'  "total_weaviate": {weaviate_count},\n')
            f.write(f'  "total_qdrant": {qdrant_count},\n')
            f.write(f'  "total_missing": {missing_count},\n')
            f.write('  "missing_chunk_ids": [\n')

            # Stream missing IDs from file to avoid loading all into memory
            first = True
            with open(MISSING_IDS_FILE, "r") as mf:
                for line in mf:
                    chunk_id = line.strip()
                    if chunk_id:
                        if not first:
                            f.write(",\n")
                        f.write(f'    "{chunk_id}"')
                        first = False

            f.write("\n  ]\n")
            f.write("}\n")

        logger.info(f"JSON output written to {OUTPUT_FILE}")

    def _cleanup_temp_files(self):
        """Remove temporary working files and checkpoint files."""
        for temp_file in [
            WEAVIATE_RAW_FILE,
            QDRANT_RAW_FILE,
            WEAVIATE_SORTED_FILE,
            QDRANT_SORTED_FILE,
            MISSING_IDS_FILE,
            WEAVIATE_CHECKPOINT,
            QDRANT_CHECKPOINT,
        ]:
            temp_file.unlink(missing_ok=True)

    async def scan_and_compute_difference(self):
        """Scan both databases in parallel and compute missing chunks.

        Pipeline:
            1. Parallel scan: Write IDs from both DBs to raw text files on disk
               (each scan is independent - one failing won't kill the other)
            2. Sort: External merge sort both files (minimal memory)
            3. Diff: Use comm to find IDs in Weaviate but not Qdrant
            4. Output: Stream results to JSON file
        """
        logger.info("=" * 80)
        logger.info("Starting parallel scan of Weaviate and Qdrant (disk-based mode)")
        logger.info("=" * 80)
        print("\n" + "=" * 60)
        print("Starting parallel scan of Weaviate and Qdrant")
        print("(Memory-efficient disk-based mode)")
        print("(Resume-enabled: will pick up from checkpoints if available)")
        print("=" * 60 + "\n")

        try:
            # Phase 1: Parallel scan to disk (independent - one can fail without killing the other)
            logger.info("Phase 1: Scanning both databases to disk...")
            results = await asyncio.gather(
                self.scan_weaviate_to_file(),
                self.scan_qdrant_to_file(),
                return_exceptions=True,
            )

            # Add newlines after progress bars
            print("\n\n")

            # Check results independently
            weaviate_result, qdrant_result = results

            if isinstance(weaviate_result, Exception):
                logger.error(f"[Weaviate] Scan failed: {weaviate_result}")
                print(f"\n[Weaviate] FAILED: {weaviate_result}")
            else:
                logger.info(f"[Weaviate] Scan succeeded: {weaviate_result:,} chunk_ids")

            if isinstance(qdrant_result, Exception):
                logger.error(f"[Qdrant] Scan failed: {qdrant_result}")
                print(f"\n[Qdrant] FAILED: {qdrant_result}")
            else:
                logger.info(f"[Qdrant] Scan succeeded: {qdrant_result:,} point IDs")

            # Both must succeed to compute the diff
            if isinstance(weaviate_result, Exception) or isinstance(qdrant_result, Exception):
                failures = []
                if isinstance(weaviate_result, Exception):
                    failures.append(f"Weaviate: {weaviate_result}")
                if isinstance(qdrant_result, Exception):
                    failures.append(f"Qdrant: {qdrant_result}")
                print("\n" + "=" * 60)
                print("One or both scans failed. Checkpoints have been saved.")
                print("Re-run the script to resume from where each scan left off.")
                print("=" * 60)
                raise RuntimeError(
                    f"Scan phase failed. Re-run to resume from checkpoints. Failures: {'; '.join(failures)}"
                )

            weaviate_raw_count = weaviate_result
            qdrant_raw_count = qdrant_result

            # Phase 2: Sort both files (sequential, uses ~256MB max via -S flag)
            logger.info("Phase 2: Sorting ID files...")
            print("Phase 2: Sorting ID files...")

            weaviate_unique = self._sort_file(
                WEAVIATE_RAW_FILE, WEAVIATE_SORTED_FILE, "Weaviate"
            )
            qdrant_unique = self._sort_file(
                QDRANT_RAW_FILE, QDRANT_SORTED_FILE, "Qdrant"
            )

            # Phase 3: Compute difference
            logger.info("Phase 3: Computing set difference...")
            print("\nPhase 3: Computing set difference...")
            missing_count = self._compute_difference()

            # Print results
            print("\n" + "=" * 60)
            print("Results:")
            print(f"  Total in Weaviate: {weaviate_unique:,}")
            print(f"  Total in Qdrant:   {qdrant_unique:,}")
            print(f"  Missing in Qdrant: {missing_count:,}")
            print("=" * 60)

            logger.info(f"Results:")
            logger.info(f"  Total in Weaviate: {weaviate_unique:,}")
            logger.info(f"  Total in Qdrant: {qdrant_unique:,}")
            logger.info(f"  Missing in Qdrant: {missing_count:,}")

            # Phase 4: Write JSON output (streamed from disk)
            logger.info("Phase 4: Writing JSON output...")
            print(f"\nPhase 4: Writing results to {OUTPUT_FILE}...")
            self._write_json_output(weaviate_unique, qdrant_unique, missing_count)

            # Cleanup temporary files
            self._cleanup_temp_files()

            logger.info("=" * 80)
            logger.info("Scan Complete!")
            logger.info(f"Results written to: {OUTPUT_FILE}")
            logger.info("=" * 80)

            print(f"Results written to: {OUTPUT_FILE}")

        except Exception as e:
            logger.error(f"Fatal error during scan: {e}", exc_info=True)
            print(f"\nError: {e}")
            raise


async def main():
    """Entry point."""
    try:
        scanner = ChunkScanner()
        await scanner.scan_and_compute_difference()
    except KeyboardInterrupt:
        logger.info("\nScan interrupted by user.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Scan failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
