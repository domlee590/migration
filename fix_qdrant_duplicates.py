#!/usr/bin/env python3
"""
Qdrant Duplicate Chunk Recovery Script

Recovers missing chunks in Qdrant caused by duplicate chunk_id bug in Weaviate.
Matches DDB.chunk_index with Weaviate.absolute_ordering to rebuild correct points.

Usage:
    python fix_qdrant_duplicates.py
"""

import os
import sys
import json
import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
from dotenv import load_dotenv
from tqdm import tqdm
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

# AWS/Database clients
import boto3
from supabase import create_client, Client as SupabaseClient
import weaviate
from weaviate.classes.query import Filter
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import PointStruct
from botocore.exceptions import ClientError

# Configuration
CHECKPOINT_FILE = Path(__file__).parent / "qdrant_recovery_checkpoint.json"
LOG_FILE = Path(__file__).parent / "qdrant_recovery.log"
ENV_FILE = Path(__file__).parent / ".env.dev"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


class QdrantRecoveryManager:
    """Manages the recovery of missing Qdrant chunks."""

    def __init__(self):
        """Initialize clients and load configuration."""
        self.load_environment()
        self.init_clients()
        self.checkpoint = self.load_checkpoint()

    def load_environment(self):
        """Load configuration from .env.dev"""
        if not ENV_FILE.exists():
            raise FileNotFoundError(f"Environment file not found: {ENV_FILE}")

        load_dotenv(ENV_FILE)

        # Supabase
        self.supabase_url = os.getenv("NEW_SUPABASE_URL")
        self.supabase_key = os.getenv("NEW_SUPABASE_KEY")

        # DynamoDB
        self.aws_region = os.getenv("AWS_REGION", "us-east-1")
        self.ddb_table_name = "chunks"

        # Weaviate
        self.weaviate_url = os.getenv("WEAVIATE_URL")
        self.weaviate_api_key = os.getenv("WEAVIATE_API_KEY")
        self.weaviate_collection = os.getenv("WEAVIATE_COLLECTION", "Text_tables")

        # Qdrant
        self.qdrant_url = os.getenv("QDRANT_URL")
        self.qdrant_api_key = os.getenv("QDRANT_API_KEY")
        self.qdrant_collection = os.getenv("QDRANT_COLLECTION", "chunks")

        # Validate required vars
        required = [
            ("NEW_SUPABASE_URL", self.supabase_url),
            ("NEW_SUPABASE_KEY", self.supabase_key),
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

    def init_clients(self):
        """Initialize database clients."""
        # Supabase
        self.supabase: SupabaseClient = create_client(
            self.supabase_url, self.supabase_key
        )
        logger.info(f"Connected to Supabase: {self.supabase_url}")

        # DynamoDB (use low-level client for attribute format)
        self.dynamodb = boto3.client("dynamodb", region_name=self.aws_region)
        logger.info(f"Connected to DynamoDB in region: {self.aws_region}")

        # Weaviate
        self.weaviate_client = weaviate.connect_to_weaviate_cloud(
            cluster_url=self.weaviate_url,
            auth_credentials=weaviate.classes.init.Auth.api_key(self.weaviate_api_key),
            skip_init_checks=False,
        )
        logger.info(f"Connected to Weaviate: {self.weaviate_url}")

        # Qdrant
        self.qdrant_client = AsyncQdrantClient(
            url=self.qdrant_url,
            api_key=self.qdrant_api_key,
        )
        logger.info(f"Connected to Qdrant: {self.qdrant_url}")

    def load_checkpoint(self) -> Dict:
        """Load checkpoint from file if exists."""
        if CHECKPOINT_FILE.exists():
            try:
                with open(CHECKPOINT_FILE, "r") as f:
                    checkpoint = json.load(f)
                logger.info(
                    f"Loaded checkpoint: {checkpoint['completed_count']}/{checkpoint['total_count']} documents completed"
                )
                return checkpoint
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}")
                return self._init_checkpoint()
        return self._init_checkpoint()

    def _init_checkpoint(self) -> Dict:
        """Initialize new checkpoint."""
        return {
            "last_completed_doc_id": None,
            "completed_doc_ids": [],
            "completed_count": 0,
            "total_count": 0,
            "total_chunks_recovered": 0,
            "errors": [],
            "timestamp": datetime.utcnow().isoformat(),
        }

    def save_checkpoint(self):
        """Save checkpoint to file."""
        try:
            self.checkpoint["timestamp"] = datetime.utcnow().isoformat()
            with open(CHECKPOINT_FILE, "w") as f:
                json.dump(self.checkpoint, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception_type((Exception,)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _query_supabase_page(self, offset: int, page_size: int):
        """Query a single page from Supabase with retry logic."""
        return (
            self.supabase.table("documents")
            .select("id,data_source_id,hash,doc_name,created_at")
            .range(offset, offset + page_size - 1)
            .execute()
        )

    def get_duplicate_documents(self) -> List[Dict]:
        """Query Supabase for all documents with duplicate hashes."""
        logger.info("Querying Supabase for documents with duplicate hashes...")

        # Fetch ALL documents with pagination
        all_documents = []
        page_size = 1000
        offset = 0

        while True:
            response = self._query_supabase_page(offset, page_size)

            if not response.data:
                break

            all_documents.extend(response.data)

            if len(response.data) < page_size:
                break

            offset += page_size
            logger.debug(f"  Fetched {len(all_documents)} documents so far...")

        logger.info(f"Fetched {len(all_documents)} total documents from Supabase")

        # Group by hash to find duplicates
        hash_groups = {}
        for doc in all_documents:
            if doc.get("hash"):
                hash_val = doc["hash"]
                if hash_val not in hash_groups:
                    hash_groups[hash_val] = []
                hash_groups[hash_val].append(doc)

        # Get only duplicate documents
        duplicate_docs = []
        for hash_val, docs in hash_groups.items():
            if len(docs) > 1:
                duplicate_docs.extend(docs)

        logger.info(
            f"Found {len(duplicate_docs)} documents with duplicate hashes ({len(hash_groups)} unique hashes with 2+ docs)"
        )
        return duplicate_docs

    def parse_ddb_item(self, item: Dict) -> Dict:
        """Parse DynamoDB attribute format into plain dict.

        Example: {"id": {"S": "abc"}} -> {"id": "abc"}
        """
        parsed = {}
        for key, value_obj in item.items():
            if "S" in value_obj:
                parsed[key] = value_obj["S"]
            elif "N" in value_obj:
                parsed[key] = int(value_obj["N"])
            elif "BOOL" in value_obj:
                parsed[key] = value_obj["BOOL"]
            elif "NULL" in value_obj:
                parsed[key] = None
            else:
                parsed[key] = value_obj
        return parsed

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception_type((ClientError, Exception)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _query_dynamodb_page(self, query_params: Dict) -> Dict:
        """Query a single page from DynamoDB with retry logic."""
        return self.dynamodb.query(**query_params)

    def get_chunks_from_dynamodb(self, doc_id: str) -> List[Dict]:
        """Query DynamoDB chunks-dev table with pagination and retry logic.

        Returns: List of parsed chunk dicts with plain values.
        """
        chunks = []
        last_key = None

        while True:
            query_params = {
                "TableName": self.ddb_table_name,
                "KeyConditionExpression": "doc_id = :doc_id",
                "ExpressionAttributeValues": {":doc_id": {"S": doc_id}},
            }

            if last_key:
                query_params["ExclusiveStartKey"] = last_key

            try:
                response = self._query_dynamodb_page(query_params)

                # Parse items
                for item in response.get("Items", []):
                    chunks.append(self.parse_ddb_item(item))

                last_key = response.get("LastEvaluatedKey")
                if not last_key:
                    break

            except Exception as e:
                logger.error(f"Error querying DynamoDB for doc {doc_id}: {e}")
                raise

        return chunks

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception_type((Exception,)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _query_weaviate_page(self, collection, doc_id: str, limit: int, offset: int):
        """Query a single page from Weaviate with retry logic."""
        return collection.query.fetch_objects(
            filters=Filter.by_property("doc_id").equal(doc_id),
            limit=limit,
            offset=offset,
            include_vector=True,
        )

    def get_chunks_from_weaviate(self, doc_id: str) -> List[Any]:
        """Query Weaviate Text_tables collection with pagination and retry logic.

        Returns: List of Weaviate objects with .properties and .vector
        """
        chunks = []
        offset = 0
        limit = 1000  # Weaviate pagination limit

        try:
            collection = self.weaviate_client.collections.get(self.weaviate_collection)

            while True:
                response = self._query_weaviate_page(collection, doc_id, limit, offset)

                if not response.objects:
                    break

                chunks.extend(response.objects)

                if len(response.objects) < limit:
                    break

                offset += limit

        except Exception as e:
            logger.error(f"Error querying Weaviate for doc {doc_id}: {e}")
            raise

        return chunks

    def match_and_build_points(
        self, ddb_chunks: List[Dict], weaviate_chunks: List[Any], doc_metadata: Dict
    ) -> List[PointStruct]:
        """Match DDB chunks to Weaviate vectors and build Qdrant points.

        Matching: ddb_chunk['chunk_index'] == weaviate_chunk.properties['absolute_ordering']
        """
        # Build vector lookup map
        vector_map = {}
        for w_chunk in weaviate_chunks:
            ordering = w_chunk.properties.get("absolute_ordering")
            if ordering is not None:
                # Weaviate returns named vectors as dict, extract the actual vector array
                vector = w_chunk.vector
                if isinstance(vector, dict) and "page_content_vector" in vector:
                    vector = vector["page_content_vector"]
                vector_map[int(ordering)] = vector

        # Match and build points
        points = []
        matched_count = 0
        unmatched_count = 0

        for ddb_chunk in ddb_chunks:
            chunk_index = ddb_chunk.get("chunk_index")
            chunk_id = ddb_chunk.get("id")

            if chunk_index is None or chunk_id is None:
                unmatched_count += 1
                continue

            vector = vector_map.get(chunk_index)
            if vector:
                # Qdrant collection uses default/unnamed vectors (just a list of floats)
                point = PointStruct(
                    id=chunk_id,
                    vector=vector,  # Plain list of floats
                    payload={
                        "doc_id": doc_metadata["id"],
                        "data_source_id": doc_metadata["data_source_id"],
                    },
                )
                points.append(point)
                matched_count += 1
            else:
                unmatched_count += 1

        logger.debug(f"  Matched: {matched_count}, Unmatched: {unmatched_count}")
        return points

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception_type((Exception,)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    async def _upsert_batch_with_retry(self, batch: List[PointStruct], batch_num: int):
        """Upsert a single batch to Qdrant with retry logic."""
        try:
            await self.qdrant_client.upsert(
                collection_name=self.qdrant_collection, points=batch, wait=True
            )
            logger.debug(
                f"Successfully upserted batch {batch_num} ({len(batch)} points)"
            )
        except Exception as e:
            logger.error(f"Failed to upsert batch {batch_num}: {e}")
            raise

    async def upsert_to_qdrant(self, points: List[PointStruct], batch_size: int = 1000):
        """Upsert points to Qdrant in batches with retry logic."""
        total = len(points)
        for i in range(0, total, batch_size):
            batch = points[i : i + batch_size]
            batch_num = i // batch_size + 1
            await self._upsert_batch_with_retry(batch, batch_num)

    async def process_document(self, doc: Dict) -> int:
        """Process a single document and recover its chunks.

        Returns: Number of chunks recovered
        """
        doc_id = doc["id"]
        logger.info(f"Processing document: {doc_id} ({doc.get('doc_name', 'unknown')})")

        try:
            # Get chunks from DynamoDB (correct chunk_ids)
            logger.debug(f"  Fetching chunks from DynamoDB...")
            ddb_chunks = self.get_chunks_from_dynamodb(doc_id)
            logger.debug(f"  Found {len(ddb_chunks)} chunks in DynamoDB")

            if not ddb_chunks:
                logger.warning(f"  No chunks found in DynamoDB for {doc_id}")
                return 0

            # Get chunks from Weaviate (correct vectors)
            logger.debug(f"  Fetching chunks from Weaviate...")
            weaviate_chunks = self.get_chunks_from_weaviate(doc_id)
            logger.debug(f"  Found {len(weaviate_chunks)} chunks in Weaviate")

            if not weaviate_chunks:
                logger.warning(f"  No chunks found in Weaviate for {doc_id}")
                return 0

            # Match and build points
            logger.debug(f"  Matching chunks...")
            points = self.match_and_build_points(ddb_chunks, weaviate_chunks, doc)

            if not points:
                logger.warning(f"  No matching chunks found for {doc_id}")
                return 0

            # Upsert to Qdrant
            logger.debug(f"  Upserting {len(points)} points to Qdrant...")
            await self.upsert_to_qdrant(points)

            logger.info(f"  Successfully recovered {len(points)} chunks for {doc_id}")
            return len(points)

        except Exception as e:
            logger.error(f"  Error processing document {doc_id}: {e}")
            self.checkpoint["errors"].append(
                {
                    "doc_id": doc_id,
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )
            return 0

    async def run(self):
        """Main recovery loop."""
        logger.info("=" * 80)
        logger.info("Starting Qdrant Duplicate Chunk Recovery")
        logger.info("=" * 80)

        try:
            # Get all duplicate documents
            duplicate_docs = self.get_duplicate_documents()

            if not duplicate_docs:
                logger.info("No duplicate documents found. Exiting.")
                return

            self.checkpoint["total_count"] = len(duplicate_docs)

            # Filter out already completed documents
            completed_set = set(self.checkpoint.get("completed_doc_ids", []))
            remaining_docs = [
                doc for doc in duplicate_docs if doc["id"] not in completed_set
            ]

            logger.info(f"Total documents: {len(duplicate_docs)}")
            logger.info(f"Already completed: {len(completed_set)}")
            logger.info(f"Remaining: {len(remaining_docs)}")

            if not remaining_docs:
                logger.info("All documents already processed. Exiting.")
                return

            # Process each document with progress bar
            with tqdm(total=len(remaining_docs), desc="Processing documents") as pbar:
                for doc in remaining_docs:
                    chunks_recovered = await self.process_document(doc)

                    # Update checkpoint
                    self.checkpoint["completed_doc_ids"].append(doc["id"])
                    self.checkpoint["last_completed_doc_id"] = doc["id"]
                    self.checkpoint["completed_count"] += 1
                    self.checkpoint["total_chunks_recovered"] += chunks_recovered

                    # Save checkpoint every 1000 documents
                    if self.checkpoint["completed_count"] % 1000 == 0:
                        self.save_checkpoint()

                    pbar.update(1)
                    pbar.set_postfix(
                        {
                            "chunks": self.checkpoint["total_chunks_recovered"],
                            "errors": len(self.checkpoint["errors"]),
                        }
                    )

            # Save final checkpoint
            self.save_checkpoint()

            logger.info("=" * 80)
            logger.info("Recovery Complete!")
            logger.info(f"  Documents processed: {self.checkpoint['completed_count']}")
            logger.info(
                f"  Chunks recovered: {self.checkpoint['total_chunks_recovered']}"
            )
            logger.info(f"  Errors: {len(self.checkpoint['errors'])}")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"Fatal error during recovery: {e}", exc_info=True)
            # Save checkpoint on error
            self.save_checkpoint()
            raise
        finally:
            # Cleanup
            if hasattr(self, "weaviate_client"):
                self.weaviate_client.close()
            if hasattr(self, "qdrant_client"):
                await self.qdrant_client.close()


async def main():
    """Entry point."""
    try:
        manager = QdrantRecoveryManager()
        await manager.run()
    except KeyboardInterrupt:
        logger.info("\nRecovery interrupted by user. Progress saved to checkpoint.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Recovery failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
