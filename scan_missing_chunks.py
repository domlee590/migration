#!/usr/bin/env python3
"""
Scan Missing Chunks Script (Memory-Efficient Version)

Scans both Weaviate and Qdrant in parallel to identify chunks that exist in
Weaviate but are missing from Qdrant. Uses disk-based sorting to avoid OOM.

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

    def _scan_weaviate_to_file(self) -> int:
        """Synchronous Weaviate scan - writes chunk_ids directly to disk.

        Returns:
            Count of chunk_ids written
        """
        logger.info(f"[Weaviate] Starting scan of {self.weaviate_collection}...")

        count = 0
        cursor = None
        batch_size = 10000

        try:
            # Connect to Weaviate
            client = weaviate.connect_to_weaviate_cloud(
                cluster_url=self.weaviate_url,
                auth_credentials=weaviate.classes.init.Auth.api_key(
                    self.weaviate_api_key
                ),
                skip_init_checks=False,
            )

            collection = client.collections.get(self.weaviate_collection)

            # Progress tracking - position=0 for top bar
            pbar = tqdm(
                desc="[Weaviate] Scanning chunk_ids",
                unit=" chunks",
                position=0,
                leave=True,
            )

            with open(WEAVIATE_RAW_FILE, "w", buffering=8 * 1024 * 1024) as f:
                while True:
                    # Retry logic for transient failures
                    for attempt in range(3):
                        try:
                            response = collection.query.fetch_objects(
                                limit=batch_size,
                                after=cursor,
                                return_properties=["chunk_id"],
                            )
                            break
                        except Exception as e:
                            if attempt == 2:
                                raise
                            logger.warning(
                                f"[Weaviate] Retry {attempt + 1}/3 after error: {e}"
                            )
                            time.sleep(2**attempt)

                    if not response.objects:
                        break

                    # Write chunk_ids directly to file
                    for obj in response.objects:
                        chunk_id = obj.properties.get("chunk_id")
                        if chunk_id:
                            f.write(chunk_id + "\n")
                            count += 1

                    # Update progress
                    pbar.update(len(response.objects))

                    # Check if we got fewer than batch_size (end of collection)
                    if len(response.objects) < batch_size:
                        break

                    # Set cursor to the last object's UUID for next iteration
                    cursor = response.objects[-1].uuid

            pbar.close()
            client.close()

            logger.info(
                f"[Weaviate] Complete: {count:,} chunk_ids written to {WEAVIATE_RAW_FILE}"
            )
            return count

        except Exception as e:
            logger.error(f"[Weaviate] Failed to scan: {e}", exc_info=True)
            tqdm.write(f"[Weaviate] Error: {e}")
            raise

    async def scan_weaviate_to_file(self) -> int:
        """Scan Weaviate chunk_ids to disk file.

        Runs synchronous Weaviate client in thread executor to not block event loop.

        Returns:
            Count of chunk_ids written
        """
        return await asyncio.to_thread(self._scan_weaviate_to_file)

    async def scan_qdrant_to_file(self) -> int:
        """Scan all point IDs from Qdrant collection directly to disk file.

        Returns:
            Count of point_ids written
        """
        logger.info(f"[Qdrant] Starting scan of {self.qdrant_collection}...")

        count = 0

        try:
            # Connect to Qdrant
            client = AsyncQdrantClient(
                url=self.qdrant_url,
                api_key=self.qdrant_api_key,
            )

            # Progress tracking - position=1 for second bar
            pbar = tqdm(
                desc="[Qdrant] Scanning point IDs",
                unit=" points",
                position=1,
                leave=True,
            )

            # Use scroll API for efficient pagination
            offset = None
            batch_size = 10000

            with open(QDRANT_RAW_FILE, "w", buffering=8 * 1024 * 1024) as f:
                while True:
                    # Retry logic for transient failures
                    for attempt in range(3):
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
                            if attempt == 2:
                                raise
                            logger.warning(
                                f"[Qdrant] Retry {attempt + 1}/3 after error: {e}"
                            )
                            await asyncio.sleep(2**attempt)

                    points, next_offset = response

                    if not points:
                        break

                    # Write point IDs directly to file
                    for point in points:
                        f.write(str(point.id) + "\n")
                        count += 1

                    # Update progress
                    pbar.update(len(points))

                    # Check if we have more pages
                    if next_offset is None:
                        break

                    offset = next_offset

            pbar.close()
            await client.close()

            logger.info(
                f"[Qdrant] Complete: {count:,} point IDs written to {QDRANT_RAW_FILE}"
            )
            return count

        except Exception as e:
            logger.error(f"[Qdrant] Failed to scan: {e}", exc_info=True)
            tqdm.write(f"[Qdrant] Error: {e}")
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
        """Remove temporary working files."""
        for temp_file in [
            WEAVIATE_RAW_FILE,
            QDRANT_RAW_FILE,
            WEAVIATE_SORTED_FILE,
            QDRANT_SORTED_FILE,
            MISSING_IDS_FILE,
        ]:
            temp_file.unlink(missing_ok=True)

    async def scan_and_compute_difference(self):
        """Scan both databases in parallel and compute missing chunks.

        Pipeline:
            1. Parallel scan: Write IDs from both DBs to raw text files on disk
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
        print("=" * 60 + "\n")

        try:
            # Phase 1: Parallel scan to disk
            logger.info("Phase 1: Scanning both databases to disk...")
            weaviate_raw_count, qdrant_raw_count = await asyncio.gather(
                self.scan_weaviate_to_file(), self.scan_qdrant_to_file()
            )

            # Add newlines after progress bars
            print("\n\n")

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
