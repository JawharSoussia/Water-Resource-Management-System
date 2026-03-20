import os
import logging
from kafka import KafkaAdminClient, KafkaClient
from kafka.admin import NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)


class KafkaManager:
    """Manages Kafka topics and configuration."""

    def __init__(self):
        """Initialize Kafka admin client."""
        bootstrap_env = os.getenv("KAFKA_BROKER", "127.0.0.1:9092")
        self.bootstrap_servers = bootstrap_env.split(",")
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers,
            client_id="data-pipeline-admin"
        )
        logger.info(f"Kafka Manager initialized for brokers: {self.bootstrap_servers}")

    def create_topics(self):
        """Create required topics."""
        topics = [
            NewTopic(
                name="aqua-realtime-water",
                num_partitions=3,
                replication_factor=1,
                topic_configs={
                    "retention.ms": "604800000",  # 7 days
                    "compression.type": "snappy"
                }
            ),
            NewTopic(
                name="aqua-realtime-weather",
                num_partitions=2,
                replication_factor=1,
                topic_configs={
                    "retention.ms": "604800000",
                    "compression.type": "snappy"
                }
            ),
            NewTopic(
                name="aqua-realtime-alerts",
                num_partitions=2,
                replication_factor=1,
                topic_configs={
                    "retention.ms": "2592000000"  # 30 days for alerts
                }
            ),
            NewTopic(
                name="data-pipeline-errors",
                num_partitions=1,
                replication_factor=1,
                topic_configs={"retention.ms": "2592000000"}  # 30 days
            ),
        ]

        try:
            # Create topics (existing topics won't error)
            res = self.admin_client.create_topics(new_topics=topics, validate_only=False)
            for topic, f in res.items():
                try:
                    f.result()  # Block until topic created
                    logger.info(f"✓ Topic created: {topic}")
                except KafkaError as e:
                    error_str = str(e)
                    if "already exists" in error_str.lower() or "TOPIC_ALREADY_EXISTS" in error_str:
                        logger.info(f"✓ Topic exists: {topic}")
                    else:
                        logger.error(f"✗ Error creating {topic}: {e}")
        except Exception as e:
            error_str = str(e)
            if "already exists" in error_str.lower():
                logger.info(f"✓ All topics already exist or created")
            else:
                logger.error(f"Error creating topics: {e}")

    def list_topics(self) -> dict:
        """List all topics."""
        try:
            # Use the internal client to get metadata
            client = self.admin_client._client
            metadata = client.cluster
            if metadata:
                topics = list(metadata.topics())
                logger.info(f"Available topics: {topics}")
            else:
                logger.info("Available topics: (none)")
            return {}
        except Exception as e:
            logger.error(f"Error listing topics: {e}")
            return {}

    def describe_consumer_groups(self):
        """Describe consumer groups and lag."""
        try:
            groups = self.admin_client.list_consumer_groups()
            for group_id, group_type in groups:
                logger.info(f"Consumer Group: {group_id} (type: {group_type})")
        except Exception as e:
            logger.error(f"Error describing consumer groups: {e}")

    def close(self):
        """Close admin client."""
        self.admin_client.close()
        logger.info("Kafka admin client closed")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    manager = KafkaManager()
    manager.create_topics()
    manager.list_topics()
    manager.describe_consumer_groups()
    manager.close()