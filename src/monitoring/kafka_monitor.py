"""Monitor Kafka consumer lag and health."""

import logging
from kafka import KafkaAdminClient
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
import os

logger = logging.getLogger(__name__)


class KafkaMonitor:
    """Monitor Kafka topics and consumer groups."""

    def __init__(self):
        """Initialize Kafka monitor."""
        self.bootstrap_servers = os.getenv("KAFKA_BROKER", "localhost:9092").split(",")
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers
        )

    def get_topic_lag(self, topic: str, group_id: str):
        """
        Get consumer lag for a topic.

        Args:
            topic: Topic name
            group_id: Consumer group ID

        Returns:
            Dictionary with lag information
        """
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id=group_id,
                consumer_timeout_ms=1000,
            )

            partitions = consumer.partitions_for_topic(topic)
            lag_info = {}

            for partition in partitions:
                tp = TopicPartition(topic, partition)
                consumer.assign([tp])
                consumer.seek_to_end(tp)
                end_offset = consumer.position(tp)
                consumer.seek_to_beginning(tp)

                lag_info[partition] = {
                    "end_offset": end_offset,
                }

            consumer.close()
            return lag_info

        except Exception as e:
            logger.error(f"Error getting topic lag: {e}")
            return {}

    def close(self):
        """Close admin client."""
        self.admin_client.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    monitor = KafkaMonitor()
    lag = monitor.get_topic_lag("aqua-realtime-water", "water-processing-group")
    logger.info(f"Topic lag: {lag}")
    monitor.close()