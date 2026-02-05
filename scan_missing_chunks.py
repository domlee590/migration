#!/usr/bin/env python3
"""
Scan Missing Chunks Script

Scans both Weaviate and Qdrant in parallel to identify chunks that exist in
Weaviate but are missing from Qdrant. Writes results to missing_chunk_ids.json.

Usage:
    python scan_missing_chunks.py
"""

import os
import sys
import json
import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Set
from dotenv import load_dotenv
from tqdm import tqdm

import weaviate
from qdrant_client import AsyncQdrantClient

# Configuration
ENV_FILE = Path(__file__).parent / ".env"
OUTPUT_FILE = Path(__file__).parent / "missing_chunk_ids.json"
LOG_FILE = Path(__file__).parent / "scan_missing_chunks.log"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


class ChunkScanner:
    """Scans Weaviate and Qdrant to identify missing chunks."""

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

    async def scan_weaviate_ids(self) -> Set[str]:
        """Scan all chunk_ids from Weaviate collection using cursor-based pagination.

        Returns:
            Set of chunk_id strings
        """
        logger.info(f"[Weaviate] Starting scan of {self.weaviate_collection}...")

        chunk_ids = set()
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

            # Progress tracking
            pbar = tqdm(desc="[Weaviate] Scanning chunk_ids", unit=" chunks")

            while True:
                # Retry logic for transient failures
                for attempt in range(3):
                    try:
                        # Use cursor-based pagination with 'after' parameter
                        # This avoids the 100k offset limit
                        response = collection.query.fetch_objects(
                            limit=batch_size,
                            after=cursor,  # Use cursor instead of offset
                            return_properties=["chunk_id"],
                        )
                        break
                    except Exception as e:
                        if attempt == 2:
                            raise
                        logger.warning(
                            f"[Weaviate] Retry {attempt + 1}/3 after error: {e}"
                        )
                        await asyncio.sleep(2**attempt)

                if not response.objects:
                    break

                # Extract chunk_ids
                for obj in response.objects:
                    chunk_id = obj.properties.get("chunk_id")
                    if chunk_id:
                        chunk_ids.add(chunk_id)

                # Update progress
                pbar.update(len(response.objects))

                # Check if we got fewer than batch_size (end of collection)
                if len(response.objects) < batch_size:
                    break

                # Set cursor to the last object's UUID for next iteration
                cursor = response.objects[-1].uuid

            pbar.close()
            client.close()

            logger.info(f"[Weaviate] Complete: {len(chunk_ids):,} chunk_ids found")
            return chunk_ids

        except Exception as e:
            logger.error(f"[Weaviate] Failed to scan: {e}", exc_info=True)
            raise

    async def scan_qdrant_ids(self) -> Set[str]:
        """Scan all point IDs from Qdrant collection.

        Returns:
            Set of point ID strings (chunk_ids)
        """
        logger.info(f"[Qdrant] Starting scan of {self.qdrant_collection}...")

        point_ids = set()

        try:
            # Connect to Qdrant
            client = AsyncQdrantClient(
                url=self.qdrant_url,
                api_key=self.qdrant_api_key,
            )

            # Progress tracking
            pbar = tqdm(desc="[Qdrant] Scanning point IDs", unit=" points")

            # Use scroll API for efficient pagination
            offset = None
            batch_size = 10000

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

                # Extract point IDs (convert to strings)
                for point in points:
                    point_ids.add(str(point.id))

                # Update progress
                pbar.update(len(points))

                # Check if we have more pages
                if next_offset is None:
                    break

                offset = next_offset

            pbar.close()
            await client.close()

            logger.info(f"[Qdrant] Complete: {len(point_ids):,} point IDs found")
            return point_ids

        except Exception as e:
            logger.error(f"[Qdrant] Failed to scan: {e}", exc_info=True)
            raise

    async def scan_and_compute_difference(self):
        """Scan both databases in parallel and compute missing chunks."""
        logger.info("=" * 80)
        logger.info("Starting parallel scan of Weaviate and Qdrant")
        logger.info("=" * 80)

        try:
            # Parallel scan
            logger.info("Launching parallel scans...")
            weaviate_ids, qdrant_ids = await asyncio.gather(
                self.scan_weaviate_ids(), self.scan_qdrant_ids()
            )

            # Compute set difference
            logger.info("Computing set difference...")
            missing_ids = weaviate_ids - qdrant_ids

            logger.info(f"Results:")
            logger.info(f"  Total in Weaviate: {len(weaviate_ids):,}")
            logger.info(f"  Total in Qdrant: {len(qdrant_ids):,}")
            logger.info(f"  Missing in Qdrant: {len(missing_ids):,}")

            # Write results to file
            output_data = {
                "scan_timestamp": datetime.utcnow().isoformat(),
                "total_weaviate": len(weaviate_ids),
                "total_qdrant": len(qdrant_ids),
                "total_missing": len(missing_ids),
                "missing_chunk_ids": sorted(list(missing_ids)),
            }

            logger.info(f"Writing results to {OUTPUT_FILE}...")
            with open(OUTPUT_FILE, "w") as f:
                json.dump(output_data, f, indent=2)

            logger.info("=" * 80)
            logger.info("Scan Complete!")
            logger.info(f"Results written to: {OUTPUT_FILE}")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"Fatal error during scan: {e}", exc_info=True)
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
