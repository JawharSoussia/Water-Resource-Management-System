"""Kafka consumer for processing streaming data and writing to PostgreSQL."""

import os
import json
import logging
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StreamConsumer:
    """Consumes and processes streaming events from Kafka."""

    def __init__(self, topic: str, group_id: str):
        """
        Initialize Kafka consumer.

        Args:
            topic: Kafka topic to consume from
            group_id: Consumer group ID
        """
        self.bootstrap_servers = os.getenv("KAFKA_BROKER", "127.0.0.1:9092").split(",")
        self.topic = topic
        self.group_id = group_id

        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )

        # Initialize database connection
        db_user = os.getenv("POSTGRES_USER", "datauser")
        db_password = os.getenv("POSTGRES_PASSWORD", "datauser")
        db_host = os.getenv("POSTGRES_HOST", "localhost")
        db_name = os.getenv("POSTGRES_DB", "data_pipeline_db")

        self.engine = create_engine(
            f"postgresql://{db_user}:{db_password}@{db_host}/{db_name}"
        )

        logger.info(f"Stream consumer initialized for topic: {topic}, group: {group_id}")

    def process_weather_event(self, event: dict):
        """
        Process weather stream event and write to PostgreSQL.

        Args:
            event: Event dictionary from Kafka
        """
        try:
            location = event.get("location")
            weather_data = event.get("weather", {})
            main_data = weather_data.get("main", {})
            wind_data = weather_data.get("wind", {})
            rain_data = weather_data.get("rain", {})
            timestamp = event.get("timestamp")

            # Insert into database using parameterized query
            with self.engine.connect() as conn:
                query = text("""
                    INSERT INTO staging.weather_stream
                    (location, temperature, humidity, pressure, wind_speed, rain_mm, timestamp)
                    VALUES (:location, :temp, :humidity, :pressure, :wind_speed, :rain_mm, :timestamp)
                """)

                conn.execute(query, {
                    "location": location,
                    "temp": main_data.get('temp'),
                    "humidity": main_data.get('humidity'),
                    "pressure": main_data.get('pressure'),
                    "wind_speed": wind_data.get('speed', 0),
                    "rain_mm": rain_data.get('1h', 0),
                    "timestamp": timestamp
                })
                conn.commit()

            logger.info(f"✓ Processed weather event for {location}")
            return True

        except Exception as e:
            logger.error(f"Error processing weather event: {e}")
            return False

    def process_water_event(self, event: dict):
        """
        Process water stream event and write to PostgreSQL.

        Args:
            event: Event dictionary from Kafka
        """
        try:
            dataset = event.get("dataset")
            water_data = event.get("water", {})
            timestamp = event.get("timestamp")

            # Insert into database using parameterized query
            with self.engine.connect() as conn:
                query = text("""
                    INSERT INTO staging.water_stream
                    (dataset_id, temperature, precipitation, timestamp)
                    VALUES (:dataset_id, :temperature, :precipitation, :timestamp)
                """)

                conn.execute(query, {
                    "dataset_id": dataset,
                    "temperature": water_data.get('temperature'),
                    "precipitation": water_data.get('precipitation'),
                    "timestamp": timestamp
                })
                conn.commit()

            logger.info(f"✓ Processed water event for dataset {dataset}")
            return True

        except Exception as e:
            logger.error(f"Error processing water event: {e}")
            return False

    def consume(self, max_messages: int = None):
        """
        Consume messages from Kafka.

        Args:
            max_messages: Max messages to consume (None = infinite)
        """
        logger.info(f"Starting consumer for topic: {self.topic}")
        messages_consumed = 0

        try:
            for message in self.consumer:
                try:
                    if "water" in self.topic:
                        success = self.process_water_event(message.value)
                    elif "weather" in self.topic:
                        success = self.process_weather_event(message.value)
                    else:
                        logger.info(f"Message: {message.value}")
                        success = True

                    if success:
                        messages_consumed += 1

                    if max_messages and messages_consumed >= max_messages:
                        logger.info(f"Reached max messages ({max_messages})")
                        break

                except Exception as e:
                    logger.error(f"Error processing message: {e}")

        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        finally:
            self.close()

    def close(self):
        """Close consumer."""
        self.consumer.close()
        self.engine.dispose()
        logger.info("Consumer closed")


def main():
    """Main entry point."""
    # Consume from weather topic
    consumer = StreamConsumer("aqua-realtime-weather", "weather-processing-group")
    consumer.consume()  # Run indefinitely


if __name__ == "__main__":
    main()
