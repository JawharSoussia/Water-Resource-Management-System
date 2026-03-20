"""Kafka producer for real-time water and weather streaming data."""

import os
import json
import time
import logging
from datetime import datetime
from typing import Dict, Any
from kafka import KafkaProducer
from kafka.errors import KafkaError
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AQUARealtimeProducer:
    """Produces real-time water and weather events to Kafka."""

    def __init__(self):
        """Initialize Kafka producer."""
        self.bootstrap_servers = os.getenv("KAFKA_BROKER", "localhost:9092").split(",")
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',  # Wait for all replicas
            retries=3,
            compression_type='snappy'
        )
        self.openweather_api_key = os.getenv("OPENWEATHER_API_KEY")
        self.locations = os.getenv("OPENWEATHER_LOCATIONS", "").split(",")
        self.noaa_datasets = os.getenv("NOAA_DATASETS", "nclimgrid").split(",")
        self.noaa_coordinates = os.getenv("NOAA_COORDINATES", "35.7942,10.1671")

        logger.info(f"AQUA Real-time producer initialized")

    def fetch_and_produce_water(self, dataset: str) -> bool:
        """
        Fetch NOAA water data and produce to Kafka.

        Args:
            dataset: NOAA dataset name (e.g., "nclimgrid")

        Returns:
            True if successful, False otherwise
        """
        try:
            # Fetch from NOAA
            lat, lon = self.noaa_coordinates.split(",")
            response = requests.get(
                "https://www.ncei.noaa.gov/api/data/access/nclimgrid",
                params={
                    "dataset": dataset.strip(),
                    "latitude": lat.strip(),
                    "longitude": lon.strip(),
                    "format": "json"
                },
                timeout=5
            )
            response.raise_for_status()
            water_data = response.json()

            # Add metadata
            event = {
                "timestamp": datetime.utcnow().isoformat(),
                "dataset": dataset,
                "water": water_data,
                "source": "noaa-water-stream"
            }

            # Send to Kafka
            partition = hash(dataset) % 3  # Partition by dataset
            future = self.producer.send(
                "aqua-realtime-water",
                value=event,
                key=dataset.encode('utf-8'),
                partition=partition
            )

            record_metadata = future.get(timeout=5)

            logger.info(
                f"✓ Produced water event for {dataset} "
                f"(partition: {record_metadata.partition}, "
                f"offset: {record_metadata.offset})"
            )
            return True

        except Exception as e:
            logger.error(f"Error producing water event for {dataset}: {e}")
            self.produce_error_event(str(e), dataset, "water")
            return False

    def fetch_and_produce_weather(self, location: str) -> bool:
        """
        Fetch OpenWeather data and produce to Kafka.

        Args:
            location: Location name (city)

        Returns:
            True if successful, False otherwise
        """
        try:
            # Fetch from OpenWeather
            params = {
                "q": location.strip(),
                "appid": self.openweather_api_key,
                "units": "metric"
            }
            response = requests.get(
                "https://api.openweathermap.org/data/2.5/weather",
                params=params,
                timeout=5
            )
            response.raise_for_status()
            weather_data = response.json()

            # Add metadata
            event = {
                "timestamp": datetime.utcnow().isoformat(),
                "location": location,
                "weather": weather_data,
                "source": "openweather-stream"
            }

            # Send to Kafka
            partition = hash(location) % 2  # Partition by location
            future = self.producer.send(
                "aqua-realtime-weather",
                value=event,
                key=location.encode('utf-8'),
                partition=partition
            )

            record_metadata = future.get(timeout=5)

            logger.info(
                f"✓ Produced weather event for {location} "
                f"(partition: {record_metadata.partition}, "
                f"offset: {record_metadata.offset})"
            )
            return True

        except Exception as e:
            logger.error(f"Error producing weather event for {location}: {e}")
            self.produce_error_event(str(e), location, "weather")
            return False

    def produce_error_event(self, error_msg: str, resource: str, resource_type: str):
        """Produce error to dead-letter queue."""
        try:
            error_event = {
                "timestamp": datetime.utcnow().isoformat(),
                "error": error_msg,
                "source": f"{resource_type}-stream",
                "resource": resource
            }
            self.producer.send("data-pipeline-errors", value=error_event)
            logger.error(f"Error event sent to dead-letter queue")
        except Exception as e:
            logger.error(f"Failed to produce error event: {e}")

    def run_continuous(self, interval_seconds: int = 300):
        """
        Run producer continuously.

        Args:
            interval_seconds: Seconds between fetches (default: 5 minutes)
        """
        logger.info(f"Starting continuous AQUA production (every {interval_seconds}s)")

        try:
            while True:
                # Fetch water data from all datasets
                for dataset in self.noaa_datasets:
                    self.fetch_and_produce_water(dataset)
                    time.sleep(1)

                # Fetch weather data from all locations
                for location in self.locations:
                    self.fetch_and_produce_weather(location)
                    time.sleep(1)

                logger.info(f"Waiting {interval_seconds}s until next batch...")
                time.sleep(interval_seconds)

        except KeyboardInterrupt:
            logger.info("Stopping producer...")
            self.close()

    def close(self):
        """Close producer."""
        self.producer.flush()
        self.producer.close()
        logger.info("AQUA real-time producer closed")


def main():
    """Main entry point."""
    producer = AQUARealtimeProducer()
    # For testing, run once
    for dataset in producer.noaa_datasets:
        producer.fetch_and_produce_water(dataset)
    for location in producer.locations:
        producer.fetch_and_produce_weather(location)
    producer.close()


if __name__ == "__main__":
    main()