"""Water data storage in MinIO."""

import os
import logging
import json
from datetime import datetime
from typing import List
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

from water_models import WaterDataBatch

load_dotenv()

logger = logging.getLogger(__name__)


class WaterDataStorage:
    """Handle water data storage in MinIO."""

    def __init__(self):
        """Initialize MinIO storage."""
        endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

        self.client = Minio(
            endpoint.replace("http://", "").replace("https://", ""),
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )

        self.bucket_raw = os.getenv("MINIO_BUCKET_RAW_WATER", "aqua-raw-water-data")
        self.bucket_staging = os.getenv("MINIO_BUCKET_STAGING_WATER", "aqua-staging-water")

        self._ensure_buckets()
        logger.info("Water Data Storage initialized")

    def _ensure_buckets(self):
        """Create buckets if they don't exist."""
        for bucket in [self.bucket_raw, self.bucket_staging]:
            try:
                if not self.client.bucket_exists(bucket):
                    self.client.make_bucket(bucket)
                    logger.info(f"✓ Created bucket: {bucket}")
                else:
                    logger.info(f"✓ Bucket exists: {bucket}")
            except S3Error as e:
                logger.error(f"Error with bucket {bucket}: {e}")

    def store_raw_water_data(self, batches: List[WaterDataBatch]):
        """Store raw water data in MinIO."""
        for batch in batches:
            try:
                # Create file path: year/month/day/station/batch.json
                date = batch.batch_timestamp
                path = (
                    f"raw/water/{date.year}/{date.month:02d}/{date.day:02d}/"
                    f"{batch.station.station_id}/{batch.batch_id}.json"
                )

                # Upload to MinIO
                data_json = batch.to_json().encode('utf-8')
                self.client.put_object(
                    self.bucket_raw,
                    path,
                    data=data_json,
                    length=len(data_json),
                    content_type="application/json"
                )

                logger.info(f"✓ Stored water data batch {batch.batch_id} to {path}")

            except S3Error as e:
                logger.error(f"Failed to store batch {batch.batch_id}: {e}")

    def store_validated_water_data(self, batches: List[WaterDataBatch]):
        """Store validated water data for transformation."""
        for batch in batches:
            try:
                if not batch.measurements:
                    continue

                date = batch.batch_timestamp
                path = (
                    f"staging/water/{date.year}/{date.month:02d}/{date.day:02d}/"
                    f"{batch.station.station_id}/{batch.batch_id}_validated.json"
                )

                data_json = batch.to_json().encode('utf-8')
                self.client.put_object(
                    self.bucket_staging,
                    path,
                    data=data_json,
                    length=len(data_json),
                    content_type="application/json"
                )

                logger.info(f"✓ Stored validated water data to {path}")

            except S3Error as e:
                logger.error(f"Failed to store validated batch: {e}")


def main():
    """Store ingestion results."""
    from noaa_water_ingestion import NOAAWaterIngestion

    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
    )

    # Fetch data
    ingestion = NOAAWaterIngestion()
    batches = ingestion.fetch_water_levels()

    # Store raw data
    storage = WaterDataStorage()
    storage.store_raw_water_data(batches)

    # Store validated data
    storage.store_validated_water_data(batches)


if __name__ == "__main__":
    main()