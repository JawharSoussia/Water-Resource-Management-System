"""Kafka producer for real-time weather streaming data."""

import os
import json
import logging
from datetime import datetime
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


class WeatherStreamProducer:
    """Produces real-time weather events to Kafka."""

    def __init__(self):
        """Initialize Kafka producer."""
        self.bootstrap_servers = os.getenv("KAFKA_BROKER", "127.0.0.1:9092").split(",")
        self.topic = "aqua-realtime-weather"
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3,
            compression_type='snappy',
            api_version_auto_timeout_ms=5000,
            connections_max_idle_ms=540000
        )
        self.openweather_api_key = os.getenv("OPENWEATHER_API_KEY", "")
        self.locations = os.getenv("OPENWEATHER_LOCATIONS", "").split(",")
        self.locations = [loc.strip() for loc in self.locations if loc.strip()]
        logger.info(f"Weather Stream Producer initialized with bootstrap servers: {self.bootstrap_servers}")
        logger.info(f"Weather locations: {self.locations}")

    def fetch_and_produce(self, location: str) -> bool:
        """
        Fetch weather data from OpenWeather API and produce to Kafka.

        Args:
            location: Location name (city)

        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.openweather_api_key:
                logger.warning("OPENWEATHER_API_KEY not set, skipping weather fetch")
                return False

            # Fetch from OpenWeather API
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
            future = self.producer.send(
                self.topic,
                value=event,
                key=location.encode('utf-8')
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
            return False

    def close(self):
        """Close producer."""
        self.producer.flush()
        self.producer.close()
        logger.info("Weather Stream Producer closed")


if __name__ == "__main__":
    import sys
    producer = WeatherStreamProducer()
    # Fetch weather for all configured locations
    if producer.locations:
        for location in producer.locations:
            producer.fetch_and_produce(location)
    else:
        logger.warning("No locations configured in OPENWEATHER_LOCATIONS")
    producer.close()
