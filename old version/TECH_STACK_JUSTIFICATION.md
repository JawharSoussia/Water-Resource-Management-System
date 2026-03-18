# Technology Stack with Justification

## 🎯 Recommended Stack for End-to-End Data Pipeline

This document outlines the technology choices from the official **Catalogue de Technologies** with detailed justifications for each component.

---

## 📊 Architecture Overview

```
Data Sources
    ↓
[Ingestion Layer] ← Python (requests) + Apache Kafka
    ↓
[Storage Layer] ← MinIO (Data Lake) + PostgreSQL (structured data)
    ↓
[Transformation Layer] ← dbt (SQL) + Pandas (Python)
    ↓
[Orchestration Layer] ← Apache Airflow
    ↓
[Visualization Layer] ← Streamlit
    ↓
End Users
```

---

## 🔄 PHASE 1: INGESTION LAYER

### ✅ Selected Technologies

#### 1. **Python (requests/httpx)** - Batch Ingestion
**Why:**
- ✅ **Simplicity:** Easy to learn and implement API calls
- ✅ **Flexibility:** Works with any REST API without vendor lock-in
- ✅ **No cost:** Completely free
- ✅ **Lightweight:** Low memory footprint for scheduled tasks
- ✅ **Community:** Extensive libraries and documentation

**When to use:**
- Pulling data from public APIs (OpenWeather, NewsAPI, etc.)
- Scheduled batch processes (hourly, daily)
- Simple, lightweight ingestion

```python
# Example: Simple API ingestion
import requests
response = requests.get('https://api.example.com/data')
data = response.json()
```

**Trade-offs:**
- ❌ Not ideal for very high-frequency (100+ requests/sec)
- ❌ No built-in retry/backoff strategies (need to implement)

---

#### 2. **Apache Kafka** - Real-Time Streaming
**Why:**
- ✅ **Industry standard:** Used by Netflix, Uber, LinkedIn
- ✅ **Scalability:** Handle millions of events/second
- ✅ **Durability:** Data persisted, never lost
- ✅ **Decoupling:** Producers/consumers independent
- ✅ **Replay:** Can reprocess historical data
- ✅ **Open source:** Free and community-supported

**When to use:**
- Live data streams (sensor data, logs, transactions)
- Need to buffer data between services
- Multiple consumers need same data
- Failure recovery is critical

**Architecture:**
```
[Real-time Data Source] → [Kafka Producer] → [Kafka Topics] → [Kafka Consumer] → [Processing]
```

**Trade-offs:**
- ❌ More complex than simple batch
- ❌ Requires infrastructure (local or cloud-hosted)
- ✅ But provides enterprise-grade reliability

---

### 🔀 Alternatives from Catalog (and why we didn't choose them)

| Technology | Use Case | Why Not Primary? |
|---|---|---|
| **AWS Kinesis** | Cloud streaming on AWS | Cloud vendor lock-in; Kafka is cloud-agnostic |
| **GCP Pub/Sub** | Cloud streaming on GCP | Only for Google Cloud users |
| **Airbyte** | Complex API connectors | Overkill for simple APIs; best for 50+ sources |
| **AWS Lambda** | Serverless batch | Pay-per-invocation; better for ad-hoc tasks |
| **Apache NiFi** | Complex data routing | UI-based; better for non-technical users |

---

## 📦 PHASE 2: STORAGE LAYER

### ✅ Selected Technologies

#### 1. **MinIO** - Data Lake
**Why:**
- ✅ **S3-compatible:** Works like AWS S3 but self-hosted
- ✅ **No cost:** Open source, unlimited storage
- ✅ **Local deployment:** Run locally with Docker
- ✅ **Perfect for learning:** Easy to understand compared to cloud S3
- ✅ **Scalable:** Can scale to petabyte-level data
- ✅ **Raw data storage:** Store data exactly as received

**When to use:**
- Storing raw, unprocessed data
- Data from multiple sources in original formats
- Archival of historical data
- Cost-effective solution (especially locally)

**How it works:**
```
[Kafka/API Data] → [MinIO Buckets] → [Data organized by source/date]
                      ↓
              [Raw data lake]
```

**Trade-offs:**
- ✅ Simple, cost-effective
- ⚠️ Not suitable for querying directly (raw format)
- ✅ Fine for our use case (we'll query from PostgreSQL)

---

#### 2. **PostgreSQL** - Structured Data/Warehouse
**Why:**
- ✅ **SQL-powerful:** Full ACID compliance, advanced queries
- ✅ **Free:** Open source, no licensing costs
- ✅ **Reliable:** Battle-tested in production for 25+ years
- ✅ **Local setup:** Easy to install locally
- ✅ **JSON support:** Can store semi-structured data (best of both worlds)
- ✅ **Not overkill:** Snowflake/BigQuery better for 100TB+, we need OLTP+OLAP

**When to use:**
- Transformed, cleaned data ready for analysis
- Real-time queries for dashboards
- ACID transactions needed
- Medium-sized datasets (GB to low TB)

**Data flow:**
```
[MinIO: Raw data] → [Transformation] → [PostgreSQL: Clean data]
```

**Trade-offs:**
- ✅ Perfect for this project size
- ❌ Not for petabyte-scale (use Snowflake if you grow)
- ✅ Hybrid approach: MinIO (raw) + PostgreSQL (processed)

---

### 🔀 Alternatives (and why we didn't choose them)

| Technology | Use Case | Why Not Primary? |
|---|---|---|
| **Snowflake** | Cloud data warehouse | $4/credit minimum cost; overkill for learning project |
| **BigQuery** | GCP's analytical DB | Cloud-locked; costs can add up quickly |
| **Delta Lake/Iceberg** | Open lakehouse format | Better for production with Spark; adds complexity |
| **DuckDB** | Lightweight analytics | Good for small files; not ideal for multi-source integration |
| **SQLite** | Simple embedded DB | Single file; doesn't scale; no concurrent access |
| **MongoDB** | NoSQL for documents | Unnecessary complexity for structured data |

---

## 🔧 PHASE 3: TRANSFORMATION LAYER

### ✅ Selected Technologies

#### 1. **dbt (Data Build Tool)** - SQL-based ELT
**Why:**
- ✅ **SQL-first:** Write transformations in pure SQL (language most people know)
- ✅ **Modularity:** Break transformations into reusable models
- ✅ **Lineage tracking:** Automatically shows which tables depend on which
- ✅ **Testing:** Built-in data quality tests
- ✅ **Documentation:** Auto-generates docs from code
- ✅ **Free:** Apache license, fully open source
- ✅ **Workflow:** Handles dependencies automatically

**When to use:**
- SQL transformations (aggregations, joins, cleansing)
- Need to document and test transformations
- Multiple dependent transformations
- Team collaboration on data transformation code

**Example transformation (dbt):**
```sql
-- models/clean_weather_data.sql
{{ config(materialized='table') }}

WITH raw_data AS (
    SELECT * FROM {{ ref('raw_weather_api') }}
),
cleaned AS (
    SELECT
        date_trunc('day', timestamp) as date,
        location,
        ROUND(temperature, 2) as temp_celsius,
        humidity,
        CASE
            WHEN humidity > 80 THEN 'High'
            WHEN humidity > 50 THEN 'Medium'
            ELSE 'Low'
        END as humidity_category
    FROM raw_data
    WHERE temperature IS NOT NULL
        AND location IS NOT NULL
)
SELECT * FROM cleaned
```

**Trade-offs:**
- ✅ Best for SQL transformations
- ⚠️ Not ideal for complex ML/ML transformations (use Python for that)

---

#### 2. **Pandas** - Python-based Transformation
**Why:**
- ✅ **Python-based:** If SQL isn't sufficient
- ✅ **Flexible:** Handle complex business logic
- ✅ **Free:** Open source
- ✅ **Easy to test:** Simple functions that return DataFrames
- ✅ **Learning curve:** Very accessible for beginners

**When to use:**
- Complex transformations beyond SQL capabilities
- Machine learning feature engineering
- Custom validation logic
- Non-tabular data processing

**Example (Pandas):**
```python
import pandas as pd

def enrich_weather_data(df):
    """Add calculated columns and clean data"""
    df['date'] = pd.to_datetime(df['timestamp'])
    df['feels_like'] = df['temperature'] - (df['wind_speed'] * 1.5)
    df['is_extreme'] = (df['temperature'] < -10) | (df['temperature'] > 40)
    return df.dropna(subset=['temperature'])
```

**Trade-offs:**
- ✅ Great for data science tasks
- ⚠️ Slower than Spark for huge datasets (100GB+)

---

### 🔀 Alternatives (and why)

| Technology | Use Case | Why Not Primary? |
|---|---|---|
| **Spark** | Big data (100GB+) | Overkill for this project; complex setup |
| **Polars** | Fast Pandas alternative | Newer; why switch from familiar Pandas? |
| **SQLMesh** | Advanced SQL transformations | More complex than dbt; dbt is more popular |
| **Dataform** | Cloud-based SQL (GCP) | Vendor lock-in to Google Cloud |

---

## ⚙️ PHASE 4: ORCHESTRATION LAYER

### ✅ Selected Technology

#### **Apache Airflow**
**Why:**
- ✅ **Gold standard:** Used by Airbnb, Lyft, Netflix
- ✅ **DAG-based:** Visualize dependencies between tasks
- ✅ **Scheduling:** Cron-like scheduling built-in
- ✅ **Monitoring:** Web UI showing status, logs, failures
- ✅ **Retries:** Auto-retry failed tasks
- ✅ **Free:** Open source
- ✅ **Ecosystem:** Works with everything (Kafka, dbt, Spark, etc.)

**When to use:**
- ANY time you need automated, scheduled pipelines
- Complex task dependencies
- Need monitoring and alerting
- Production-grade orchestration

**Airflow DAG Example:**
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weather_pipeline',
    default_args=default_args,
    schedule_interval='0 */6 * * *',  # Every 6 hours
    start_date=datetime(2024, 1, 1),
) as dag:

    # Task 1: Ingest from API
    task_ingest = PythonOperator(
        task_id='fetch_weather_api',
        python_callable=fetch_from_api,
    )

    # Task 2: Send to Kafka
    task_kafka = PythonOperator(
        task_id='send_to_kafka',
        python_callable=send_to_kafka,
    )

    # Task 3: Transform with dbt
    task_transform = BashOperator(
        task_id='run_dbt_transformations',
        bash_command='cd /dbt && dbt run',
    )

    # Task 4: Refresh dashboard
    task_refresh = PythonOperator(
        task_id='refresh_dashboard',
        python_callable=refresh_streamlit,
    )

    # Define dependencies
    task_ingest >> task_kafka >> task_transform >> task_refresh
```

**How it looks:**
```
[6:00 AM] Fetch API
    ↓ (wait for completion)
[6:05 AM] Send to Kafka
    ↓ (wait for completion)
[6:10 AM] Run dbt transformations
    ↓ (wait for completion)
[6:15 AM] Refresh dashboard
```

**Web UI:**
- Shows all DAGs
- Task status (success, failed, running)
- Logs for each task
- Can retry/resume failed tasks

**Trade-offs:**
- ✅ Very powerful
- ⚠️ Slight learning curve (but worth it)
- ✅ Lightweight to run locally

---

### 🔀 Alternatives

| Technology | Use Case | Why Not Primary? |
|---|---|---|
| **Prefect** | Modern alternative to Airflow | Both great; Airflow more popular in industry |
| **Dagster** | Data-aware orchestration | More complex; overkill for this project |
| **Mage** | Modern, simpler UI | Good option; but Airflow more popular |
| **Cron + Scripts** | Simple shell scheduling | Works but no UI/monitoring; not professional |
| **AWS Step Functions** | Cloud-native (AWS only) | Vendor lock-in; works only on AWS |

---

## 📊 PHASE 5: VISUALIZATION LAYER

### ✅ Selected Technology

#### **Streamlit**
**Why:**
- ✅ **Super fast:** Build dashboards in pure Python in minutes
- ✅ **No frontend skills needed:** Write Python, not HTML/CSS/JavaScript
- ✅ **Interactive:** Sliders, dropdowns, buttons built-in
- ✅ **Free:** Open source and free hosting options
- ✅ **Easy deployment:** Deploy to Streamlit Cloud in 2 clicks
- ✅ **Perfect for data:** Designed specifically for data apps
- ✅ **Hot reload:** Changes appear instantly while developing

**When to use:**
- Dashboards for internal/learning use
- Quick prototypes
- Data exploration tools
- Analytics dashboards

**Streamlit Example:**
```python
import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

# Page configuration
st.set_page_config(page_title="Weather Dashboard", layout="wide")

# Title
st.title("📊 Real-Time Weather Pipeline Dashboard")

# Connect to PostgreSQL
engine = create_engine('postgresql://user:password@localhost/weather_db')
df = pd.read_sql("SELECT * FROM weather_clean ORDER BY date DESC LIMIT 1000", engine)

# Filters
with st.sidebar:
    st.header("Filters")
    selected_location = st.multiselect(
        "Select Location(s)",
        options=df['location'].unique(),
        default=df['location'].unique()
    )

# Filter data
filtered_df = df[df['location'].isin(selected_location)]

# Metrics
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Avg Temperature", f"{filtered_df['temperature'].mean():.1f}°C")
with col2:
    st.metric("Avg Humidity", f"{filtered_df['humidity'].mean():.0f}%")
with col3:
    st.metric("Max Wind Speed", f"{filtered_df['wind_speed'].max():.1f} m/s")
with col4:
    st.metric("Records Count", len(filtered_df))

# Charts
col1, col2 = st.columns(2)

with col1:
    # Temperature trend
    fig_temp = px.line(filtered_df, x='date', y='temperature', color='location',
                       title="Temperature Trend")
    st.plotly_chart(fig_temp, use_container_width=True)

with col2:
    # Humidity distribution
    fig_humidity = px.box(filtered_df, x='location', y='humidity',
                          title="Humidity Distribution")
    st.plotly_chart(fig_humidity, use_container_width=True)

# Data table
st.subheader("📋 Raw Data")
st.dataframe(filtered_df.sort_values('date', ascending=False))
```

**Deployment:**
```
1. Push to GitHub
2. Go to share.streamlit.io
3. Select your repo
4. Done! Dashboard is live
```

**Trade-offs:**
- ✅ Easiest option for this project
- ⚠️ Less customization than custom HTML/React
- ✅ Fast enough for 99% of use cases

---

### 🔀 Alternatives

| Technology | Use Case | Why Not Primary? |
|---|---|---|
| **Metabase** | Beautiful self-hosted BI tool | Slightly more setup; great alternative |
| **Superset** | Apache's BI tool | More features; steeper learning curve |
| **PowerBI** | Enterprise reporting | Requires Office license; overkill for this |
| **Tableau** | Premium analytics | $$$ expensive; professional market |
| **Dash (Plotly)** | Python-based dashboards | More complex than Streamlit; similar results |
| **FastAPI + React** | Custom API + frontend | Way more complex; unnecessary for this project |
| **Grafana** | Monitoring dashboards | Better for infrastructure monitoring, not data analytics |

---

## 📋 COMPLETE TECHNOLOGY STACK SUMMARY

### Final Selection:

```
┌─────────────────────────────────────────────────────────────┐
│                  END-TO-END DATA PIPELINE                   │
├──────────────────┬──────────────┬─────────────────────────┤
│ LAYER            │ PRIMARY      │ ALTERNATIVE/SECONDARY   │
├──────────────────┼──────────────┼─────────────────────────┤
│ Ingestion (Batch)│ Python       │ Airflow + Python        │
│ Ingestion (Real) │ Kafka        │ Kinesis, Pub/Sub        │
│ Storage (Raw)    │ MinIO        │ S3, GCS, ADLS           │
│ Storage (Cleaned)│ PostgreSQL   │ DuckDB, Snowflake       │
│ Transform (SQL)  │ dbt          │ SQLMesh, Dataform       │
│ Transform (Code) │ Pandas       │ Polars, Spark           │
│ Orchestration    │ Airflow      │ Prefect, Dagster       │
│ Visualization    │ Streamlit    │ Metabase, Superset     │
│ Deployment       │ Docker       │ Kubernetes             │
│ Monitoring (opt) │ Grafana      │ DataDog, CloudWatch    │
└──────────────────┴──────────────┴─────────────────────────┘
```

---

## 🎯 KEY JUSTIFICATIONS

### Why This Stack Works Together:

#### 1. **Cost-Effective**
- ✅ All open source (free)
- ✅ Can run locally (no cloud bills)
- ✅ Zero vendor lock-in
- ✅ Can deploy to free cloud tiers if needed

#### 2. **Scalability**
- ✅ Start locally with Kafka + PostgrSQL
- ✅ Migrate to cloud without code changes
- ✅ Swappable components (PostgreSQL → Snowflake, etc.)

#### 3. **Learning Value**
- ✅ Each tool is industry-standard
- ✅ Skills transferable to real jobs
- ✅ Not using niche/exotic technologies
- ✅ Strong community support

#### 4. **Coherent Workflow**
```
Python (simple) → Kafka (stream) → MinIO (raw) → PostgreSQL (clean)
                                   ↓
                            dbt (transform)
                                   ↓
                            Airflow (schedule)
                                   ↓
                            Streamlit (visualize)
```

#### 5. **Deployment-Ready**
- ✅ Docker-compose can orchestrate all services
- ✅ Works locally identical to production
- ✅ Easy to push to Heroku, Render, AWS EC2

---

## 📦 Why NOT Certain Popular Technologies:

### ❌ Spark
- **Reason:** Adds complexity; better for 100GB+ data
- **Our use case:** Pandas sufficient for 1-10GB
- **But:** If data grows, easy migration: dbt + Spark

### ❌ Snowflake/BigQuery
- **Reason:** Monthly costs start at $100+
- **Our use case:** PostgreSQL free and sufficient
- **But:** If scaling, simple migration: change dbt connection

### ❌ Cloud-locked (AWS Lambda, GCP Functions)
- **Reason:** Creates vendor lock-in
- **Our use case:** Airflow cloud-agnostic
- **But:** Airflow runs on any cloud if needed

### ❌ Complex Tools (NiFi, Talend)
- **Reason:** Overkill; learning tools for non-technical users
- **Our use case:** We're technical; Python + dbt better

---

## 🚀 Deployment Architecture

### Local Development (Docker Compose):
```yaml
version: '3.8'
services:
  postgres:
    image: postgres:15

  minio:
    image: minio/minio

  kafka:
    image: confluentinc/cp-kafka

  airflow:
    image: apache/airflow

  streamlit:
    image: custom-streamlit-app
```

### Production Deployment:
```
GitHub → Docker Build → Push to Registry → Deploy to:
  - Heroku/Render (easiest)
  - AWS EC2 (most control)
  - DigitalOcean (good middle ground)
  - GCP Cloud Run (serverless)
```

---

## ✅ Stack Validation Against Requirements

| Requirement | Stack | Met? |
|---|---|---|
| 2+ sources | Python API + Kafka | ✅ |
| Batch ingestion | Python + Airflow | ✅ |
| Streaming | Kafka | ✅ |
| Storage justified | MinIO (raw) + PostgreSQL (clean) | ✅ |
| 3+ transformations | dbt models + Pandas functions | ✅ |
| Orchestration | Airflow DAGs | ✅ |
| Dashboard/API | Streamlit | ✅ |
| Tech justification | This document | ✅ |
| Docker deployment | docker-compose.yml | ✅ |
| Logs + retry | Airflow built-in + Python logging | ✅ |
| 5+ tests | Pytest + dbt tests | ✅ |
| README + diagram | Git repository | ✅ |
| Git repository | Public GitHub | ✅ |

**All requirements covered! ✅**

---

## 🎓 Next Steps

1. **Confirm technologies** - Review this document
2. **Choose data subject** - Pick your API/data source
3. **Set up locally:**
   ```bash
   docker-compose up -d
   # PostgreSQL, Kafka, MinIO running
   ```
4. **Create Python ingestion** - Fetch your first API call
5. **Build dbt models** - Transform the data
6. **Create Airflow DAG** - Automate the pipeline
7. **Build Streamlit dashboard** - Visualize results
8. **Deploy to cloud** - Make it publicly accessible

---

## 📚 Learning Path

| Week | Tool | Resource |
|---|---|---|
| Week 1 | Python requests | Real Python + official docs |
| Week 2 | PostgreSQL + SQL | Mode Analytics SQL tutorial |
| Week 3 | dbt | dbt Learn documentation |
| Week 4 | Kafka | Kafka for Beginners (YouTube) |
| Week 5 | Airflow | Airflow Fundaments course |
| Week 6 | Streamlit | Official Streamlit tutorial |

Good luck! 🚀
