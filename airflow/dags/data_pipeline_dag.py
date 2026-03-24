"""
Main data pipeline DAG - Orchestrates entire ETL pipeline.

Schedule: Daily at 6:00 AM UTC
Email alerts on failure
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import logging

logger = logging.getLogger(__name__)

# Default arguments for all tasks
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

# DAG definition
dag = DAG(
    'data_pipeline_end_to_end',
    default_args=default_args,
    description='Complete end-to-end data pipeline',
    schedule_interval='0 6 * * *',  # Daily at 6 AM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['production', 'data'],
)


# Python functions for tasks
def setup_pipeline():
    """Setup: Log pipeline start."""
    logger.info("=== DATA PIPELINE STARTED ===")
    logger.info(f"Execution date: {datetime.utcnow()}")
    return {
        'timestamp': datetime.utcnow().isoformat(),
        'status': 'started'
    }


def fetch_weather_data():
    """Task 1: Fetch weather data from API."""
    import subprocess
    result = subprocess.run(
        ['python', 'src/ingestion/weather_ingestion.py'],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        logger.error(f"Weather ingestion failed: {result.stderr}")
        raise Exception("Weather ingestion failed")
    logger.info(f"Weather ingestion successful")
    return result.stdout


def fetch_water_data():
    """Task 2: Fetch NOAA Global Water API data."""
    import subprocess
    result = subprocess.run(
        ['python', 'src/ingestion/water_ingestion.py'],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        logger.error(f"Water API ingestion failed: {result.stderr}")
        raise Exception("Water API ingestion failed")
    logger.info(f"Water API ingestion successful")
    return result.stdout


def produce_weather_stream():
    """Task 3: Produce weather events to Kafka."""
    import subprocess
    result = subprocess.run(
        ['python', 'src/ingestion/kafka_weather_producer.py'],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        logger.error(f"Weather stream production failed: {result.stderr}")
        raise Exception("Weather stream production failed")
    logger.info("Weather stream production successful")
    return result.stdout


def check_data_quality(**context):
    """Task 4: Run data quality checks."""
    import subprocess
    result = subprocess.run(
        ['pytest', 'tests/', '-v', '--tb=short'],
        capture_output=True,
        text=True
    )
    logger.info(f"Data quality checks: {result.stdout}")
    if result.returncode != 0:
        logger.warning(f"Some tests failed: {result.stderr}")
    return {
        'tests_passed': result.returncode == 0
    }


def enrich_data():
    """Task 5: Enrich data with Pandas."""
    import subprocess
    result = subprocess.run(
        ['python', 'src/transformation/data_enrichment.py'],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        logger.error(f"Data enrichment failed: {result.stderr}")
        raise Exception("Data enrichment failed")
    logger.info("Data enrichment successful")
    return result.stdout


def cleanup_pipeline():
    """Cleanup: Log pipeline completion."""
    logger.info("=== DATA PIPELINE COMPLETED ===")
    return {
        'timestamp': datetime.utcnow().isoformat(),
        'status': 'completed'
    }


# Task definitions
with dag:

    # Setup task
    start = PythonOperator(
        task_id='setup_pipeline',
        python_callable=setup_pipeline,
        op_kwargs={},
    )

    # Ingestion task group
    with TaskGroup('ingestion') as ingestion_group:

        # Batch ingestion tasks
        fetch_weather = PythonOperator(
            task_id='fetch_weather_api',
            python_callable=fetch_weather_data,
            retries=2,
        )

        fetch_water = PythonOperator(
            task_id='fetch_water_api',
            python_callable=fetch_water_data,
            retries=2,
        )

        # Streaming tasks
        produce_stream = PythonOperator(
            task_id='produce_kafka_stream',
            python_callable=produce_weather_stream,
            retries=2,
        )

        # Task dependencies within group
        [fetch_weather, fetch_water] >> produce_stream

    # Data quality checks
    quality_check = PythonOperator(
        task_id='data_quality_checks',
        python_callable=check_data_quality,
    )

    # Transformation: Run dbt
    dbt_run = BashOperator(
        task_id='run_dbt_transformation',
        bash_command='cd dbt && dbt run',
    )

    dbt_test = BashOperator(
        task_id='run_dbt_tests',
        bash_command='cd dbt && dbt test',
    )

    # Data enrichment
    enrichment = PythonOperator(
        task_id='enrich_data',
        python_callable=enrich_data,
    )

    # Visualization: Refresh Streamlit (optional)
    # refresh_dashboard = BashOperator(
    #     task_id='refresh_streamlit_dashboard',
    #     bash_command='streamlit run app.py',
    # )

    # Cleanup
    end = PythonOperator(
        task_id='cleanup_pipeline',
        python_callable=cleanup_pipeline,
    )

    # Define task dependencies (execution order)
    start >> ingestion_group >> quality_check
    quality_check >> dbt_run >> dbt_test
    dbt_test >> enrichment >> end


# Task documentation
start.doc = """
### Setup Task

Log pipeline execution start and parameters.
"""

dbt_run.doc = """
### dbt Transformation

Run dbt models to create staging and analytics tables.
- Clean and standardize raw data
- Create aggregated analytics tables
"""

dbt_test.doc = """
### dbt Tests

Validate data quality and business rules.
Fails if any tests don't pass.
"""

enrichment.doc = """
### Data Enrichment

Add calculated fields and business categorizations.
Includes heat index calculations, quality scores, etc.
"""