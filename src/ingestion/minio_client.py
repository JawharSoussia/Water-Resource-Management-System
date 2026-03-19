"""MinIO client for uploading raw data to object storage."""

import os
import json
from datetime import datetime
from pathlib import Path
from minio import Minio
from minio.error import S3Error
import logging

logger = logging.getLogger(__name__)


class MinIOClient:
    """Handles uploads to MinIO object storage."""

    def __init__(self):
        """Initialize MinIO client from environment variables."""
        self.endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        self.access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        self.bucket = os.getenv("MINIO_BUCKET_RAW", "raw-data")

        # Parse endpoint (remove http://)
        endpoint = self.endpoint.replace("http://", "").replace("https://", "")

        self.client = Minio(
            endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=False,  # True if using HTTPS
        )

        logger.info(f"MinIO client initialized: {endpoint}")

    def ensure_bucket_exists(self):
        """Create bucket if it doesn't exist."""
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
                logger.info(f"Created bucket: {self.bucket}")
            else:
                logger.info(f"Bucket exists: {self.bucket}")
        except S3Error as e:
            logger.error(f"Error creating bucket: {e}")
            raise

    def upload_data(self, source_name: str, data: dict) -> str:
        """
        Upload JSON data to MinIO.

        Args:
            source_name: API source name (e.g., "openweather")
            data: Dictionary containing the API response

        Returns:
            S3 object path where data was uploaded
        """
        try:
            # Create object path: raw-data/source_name/YYYY/MM/DD/HH-MM-SS.json
            now = datetime.utcnow()
            object_path = (
                f"raw/{source_name}/"
                f"{now.year:04d}/{now.month:02d}/{now.day:02d}/"
                f"{now.hour:02d}-{now.minute:02d}-{now.second:02d}.json"
            )

            # Convert data to JSON
            data_json = json.dumps(data, indent=2).encode("utf-8")

            # Upload to MinIO
            self.client.put_object(
                self.bucket,
                object_path,
                data=io.BytesIO(data_json),
                length=len(data_json),
                content_type="application/json",
            )

            logger.info(f"Uploaded to MinIO: s3://{self.bucket}/{object_path}")
            return object_path

        except S3Error as e:
            logger.error(f"Error uploading to MinIO: {e}")
            raise

    def list_uploaded_files(self, source_name: str) -> list:
        """List all files uploaded from a specific source."""
        try:
            objects = self.client.list_objects(
                self.bucket, prefix=f"raw/{source_name}/", recursive=True
            )
            return [obj.object_name for obj in objects]
        except S3Error as e:
            logger.error(f"Error listing objects: {e}")
            return []


# Required import
import io