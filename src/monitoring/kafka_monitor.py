"""Monitor Kafka consumer lag and health."""

import logging
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.structs import TopicPartition
import os

logger = logging.getLogger(__name__)


class KafkaMonitor:
    """Monitor Kafka topics and consumer groups."""

    def __init__(self):
        """Initialize Kafka monitor."""
        self.bootstrap_servers = os.getenv("KAFKA_BROKER", "127.0.0.1:9092").split(",")
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers
        )
        logger.info(f"Kafka Monitor initialized with brokers: {self.bootstrap_servers}")

    def get_consumer_lag(self, topic: str, group_id: str):
        """
        Get consumer lag for a topic and consumer group.

        Args:
            topic: Topic name
            group_id: Consumer group ID

        Returns:
            Dictionary with lag information per partition
        """
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id=group_id,
                consumer_timeout_ms=5000,
            )

            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                logger.warning(f"No partitions found for topic: {topic}")
                consumer.close()
                return {}

            lag_info = {}

            for partition in partitions:
                tp = TopicPartition(topic, partition)
                consumer.assign([tp])

                # Get current offset
                consumer.seek_to_end(tp)
                end_offset = consumer.position(tp)

                # Get committed offset for consumer group
                committed_offset = consumer.committed(tp)
                current_offset = committed_offset if committed_offset else 0

                # Calculate lag
                lag = end_offset - current_offset if current_offset else end_offset

                lag_info[partition] = {
                    "current_offset": current_offset,
                    "end_offset": end_offset,
                    "lag": lag
                }

                logger.info(f"Topic: {topic}, Partition: {partition}, "
                           f"Current Offset: {current_offset}, End Offset: {end_offset}, "
                           f"Lag: {lag}")

            consumer.close()
            return lag_info

        except Exception as e:
            logger.error(f"Error getting consumer lag: {e}")
            return {}

    def get_topic_lag(self, topic: str, group_id: str):
        """
        Get total consumer lag for a topic.

        Args:
            topic: Topic name
            group_id: Consumer group ID

        Returns:
            Total lag across all partitions
        """
        lag_info = self.get_consumer_lag(topic, group_id)
        total_lag = sum(p["lag"] for p in lag_info.values()) if lag_info else 0
        return total_lag

    def list_consumer_groups(self):
        """
        List all consumer groups.

        Returns:
            List of consumer group IDs
        """
        try:
            groups = self.admin_client.list_consumer_groups()
            group_list = [group_id for group_id, group_type in groups]
            logger.info(f"Consumer groups: {group_list}")
            return group_list
        except Exception as e:
            logger.error(f"Error listing consumer groups: {e}")
            return []

    def monitor_lag(self, topic: str, group_id: str):
        """
        Monitor and display consumer lag for a topic.

        Args:
            topic: Topic name
            group_id: Consumer group ID
        """
        lag_info = self.get_consumer_lag(topic, group_id)
        total_lag = sum(p["lag"] for p in lag_info.values()) if lag_info else 0

        logger.info(f"\n{'='*60}")
        logger.info(f"Consumer Lag Report")
        logger.info(f"{'='*60}")
        logger.info(f"Topic: {topic}")
        logger.info(f"Consumer Group: {group_id}")
        logger.info(f"Total Lag: {total_lag} messages")
        logger.info(f"Partition Details:")
        for partition, info in lag_info.items():
            logger.info(f"  Partition {partition}: "
                       f"Lag={info['lag']}, "
                       f"Current={info['current_offset']}, "
                       f"End={info['end_offset']}")
        logger.info(f"{'='*60}\n")

    def close(self):
        """Close admin client."""
        self.admin_client.close()
        logger.info("Kafka monitor closed")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    monitor = KafkaMonitor()

    # Monitor weather stream
    monitor.monitor_lag("aqua-realtime-weather", "weather-processing-group")

    # Monitor water stream
    monitor.monitor_lag("aqua-realtime-water", "water-processing-group")

    # List all consumer groups
    groups = monitor.list_consumer_groups()

    monitor.close()
