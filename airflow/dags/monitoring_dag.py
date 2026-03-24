"""
Monitoring DAG - Checks pipeline health hourly.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pipeline_monitoring',
    default_args=default_args,
    description='Monitor pipeline health',
    schedule_interval='0 * * * *',  # Every hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['monitoring'],
)


def check_postgres_connection():
    """Check PostgreSQL is accessible."""
    import psycopg2
    from psycopg2 import sql
    import os

    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            database=os.getenv("POSTGRES_DB", "data_pipeline_db"),
            user=os.getenv("POSTGRES_USER", "datauser"),
            password=os.getenv("POSTGRES_PASSWORD", "datauser"),
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        conn.close()
        logger.info("✓ PostgreSQL connection successful")
        return True
    except Exception as e:
        logger.error(f"✗ PostgreSQL connection failed: {e}")
        raise


def check_minio_connection():
    """Check MinIO is accessible."""
    from minio import Minio
    import os

    try:
        minio = Minio(
            os.getenv("MINIO_ENDPOINT", "localhost:9000").replace("http://", ""),
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            secure=False,
        )
        minio.list_buckets()
        logger.info("✓ MinIO connection successful")
        return True
    except Exception as e:
        logger.error(f"✗ MinIO connection failed: {e}")
        raise


def check_kafka_connection():
    """Check Kafka is accessible."""
    from kafka import KafkaClient
    import os

    try:
        client = KafkaClient(
            bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:9092")
        )
        client.close()
        logger.info("✓ Kafka connection successful")
        return True
    except Exception as e:
        logger.error(f"✗ Kafka connection failed: {e}")
        raise


def check_data_freshness():
    """Check if recent data exists."""
    import psycopg2
    from datetime import datetime, timedelta
    import os

    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            database=os.getenv("POSTGRES_DB", "data_pipeline_db"),
            user=os.getenv("POSTGRES_USER", "datauser"),
            password=os.getenv("POSTGRES_PASSWORD", "datauser"),
        )
        cursor = conn.cursor()

        # Check if data from last 24 hours exists
        cursor.execute("""
            SELECT COUNT(*) as record_count
            FROM staging.stg_weather
            WHERE created_at > NOW() - INTERVAL '24 hours'
        """)
        count = cursor.fetchone()[0]
        conn.close()

        if count > 0:
            logger.info(f"✓ Data freshness check passed ({count} records)")
            return True
        else:
            logger.warning("✗ No recent data found")
            return False

    except Exception as e:
        logger.error(f"✗ Data freshness check failed: {e}")
        raise


with dag:

    check_postgres = PythonOperator(
        task_id='check_postgres',
        python_callable=check_postgres_connection,
    )

    check_minio = PythonOperator(
        task_id='check_minio',
        python_callable=check_minio_connection,
    )

    check_kafka = PythonOperator(
        task_id='check_kafka',
        python_callable=check_kafka_connection,
    )

    check_freshness = PythonOperator(
        task_id='check_data_freshness',
        python_callable=check_data_freshness,
    )

    # All checks run in parallel
    [check_postgres, check_minio, check_kafka] >> check_freshness