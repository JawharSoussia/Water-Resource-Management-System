# Data Pipeline End-to-End Project

## 🎯 Project Overview

A **complete data pipeline project** focused on **water resource management for Tunisia**.

## 📊 Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                  DATA PIPELINE ARCHITECTURE                 │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  Data Sources                                               │
│  ├── API 1 (e.g., OpenWeather)                             │
│  └── API 2 (e.g., waterAPI)                                 │
│       ↓                                                      │
│  ┌─────────────────────────────────────────────────────┐   │
│  │           INGESTION LAYER                           │   │
│  │  ┌──────────────────────────────────────────────┐  │   │
│  │  │ Python Scripts (Batch Ingestion)             │  │   │
│  │  │ Scheduled via Airflow (6 hourly)             │  │   │
│  │  └──────────────────────────────────────────────┘  │   │
│  │  ┌──────────────────────────────────────────────┐  │   │
│  │  │ Kafka (Real-time Streaming)                  │  │   │
│  │  │ Receives live data streams                   │  │   │
│  │  └──────────────────────────────────────────────┘  │   │
│  └─────────────────────────────────────────────────────┘   │
│       ↓                                                      │
│  ┌─────────────────────────────────────────────────────┐   │
│  │           STORAGE LAYER                             │   │
│  │  ┌──────────────────────────────────────────────┐  │   │
│  │  │ MinIO (Data Lake - Raw Data)                 │  │   │
│  │  │ CSV, JSON storage as received                │  │   │
│  │  └──────────────────────────────────────────────┘  │   │
│  │  ┌──────────────────────────────────────────────┐  │   │
│  │  │ PostgreSQL (Data Warehouse - Clean Data)     │  │   │
│  │  │ Structured, queryable tables                 │  │   │
│  │  └──────────────────────────────────────────────┘  │   │
│  └─────────────────────────────────────────────────────┘   │
│       ↓                                                      │
│  ┌─────────────────────────────────────────────────────┐   │
│  │       TRANSFORMATION LAYER                          │   │
│  │  ┌──────────────────────────────────────────────┐  │   │
│  │  │ dbt (SQL-based transformations)              │  │   │
│  │  │ Models: cleaning, aggregation, enrichment    │  │   │
│  │  └──────────────────────────────────────────────┘  │   │
│  │  ┌──────────────────────────────────────────────┐  │   │
│  │  │ Pandas (Python transformations)              │  │   │
│  │  │ Complex business logic                       │  │   │
│  │  └──────────────────────────────────────────────┘  │   │
│  └─────────────────────────────────────────────────────┘   │
│       ↓                                                      │
│  ┌─────────────────────────────────────────────────────┐   │
│  │      ORCHESTRATION LAYER                            │   │
│  │  Apache Airflow                                    │   │
│  │  ├─ Task: Ingest from API (6:00 AM)              │   │
│  │  ├─ Task: Send to Kafka (6:15 AM)                │   │
│  │  ├─ Task: Run dbt (6:30 AM)                      │   │
│  │  └─ Task: Refresh Dashboard (6:45 AM)            │   │
│  └─────────────────────────────────────────────────────┘   │
│       ↓                                                      │
│  ┌─────────────────────────────────────────────────────┐   │
│  │     VISUALIZATION LAYER                             │   │
│  │  Streamlit Dashboard                              │   │
│  │  ├─ Real-time metrics                             │   │
│  │  ├─ Interactive charts                            │   │
│  │  └─ Data exploration                              │   │
│  └─────────────────────────────────────────────────────┘   │
│       ↓                                                      │
│  End Users (Web Browser)                                 │   │
│                                                          │   │
└──────────────────────────────────────────────────────────┘
```

## 🛠 Technology Stack

| Component | Technology | Reason |
|---|---|---|
| Ingestion (Batch) | Python + requests | Simple, flexible, free |
| Ingestion (Stream) | Apache Kafka | Industry standard real-time |
| Storage (Raw) | MinIO | Free S3-compatible storage |
| Storage (Clean) | PostgreSQL | Reliable, powerful, free |
| Transformation | dbt + Pandas | SQL + Python flexibility |
| Orchestration | Apache Airflow | Professional scheduler |
| Visualization | Streamlit | Fast Python dashboard |
| Deployment | Docker | Reproducible environments |

## 🚀 Getting Started

### Prerequisites
- Python 3.9+
- Docker & Docker Compose
- Git

### Setup Instructions

1. **Clone and navigate to project:**
   ```bash
   cd projet
   ```

2. **Create virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables:**
   ```bash
   cp .env.example .env
   # Edit .env with your API keys
   ```

5. **Start services:**
   ```bash
   docker-compose up -d
   ```

6. **Verify services are running:**
   ```bash
   docker-compose ps
   ```

### Access Points

- **PostgreSQL:** `localhost:5432`
- **MinIO Console:** http://localhost:9001
- **Kafka:** `localhost:9092`
- **Airflow:** http://localhost:8080 (after setup)
- **Streamlit Dashboard:** http://localhost:8501 (after setup)

## 📊 Data Sources

[To be completed - list your APIs and data sources]

## 📝 Project Structure

```
projet/
├── src/                 # Python source code
│   ├── ingestion/      # Data ingestion scripts
│   ├── transformation/ # Data processing
│   └── orchestration/  # Airflow DAGs
├── data/               # Local data storage
├── dbt/                # dbt models
├── airflow/            # Airflow configuration
├── tests/              # Unit and integration tests
├── config/             # Configuration files
└── docs/               # Documentation
```

## 🔄 Pipeline Workflow

[To be completed - describe your specific workflow]

## ✅ Testing

Run tests with:
```bash
pytest tests/ -v
```

## 📦 Deployment

[To be completed - deployment instructions]

## 📚 Documentation

- [Setup Guide](docs/setup.md)
- [Architecture](docs/architecture.md)
- [Development Guide](docs/development.md)

## 📧 Contact

soussiajawhar2@gmail.com

