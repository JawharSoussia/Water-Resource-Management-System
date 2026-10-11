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
### Local Setup

1. **Clone repository:**
   ```bash
   git clone https://github.com/YOUR_USERNAME/data-pipeline.git
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

4. **Configure environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your API keys
   ```

5. **Start Docker services:**
   ```bash
   docker-compose up -d
   ```

6. **Initialize Airflow:**
   ```bash
   airflow db init
   airflow users create --username admin --password admin123 \
       --firstname Admin --lastname User --role Admin \
       --email admin@example.com
   ```

7. **Run Streamlit dashboard:**
   ```bash
   streamlit run streamlit_app.py
   # Opens at http://localhost:8501

  

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
Start
  ├─ Fetch Weather API (parallel)
  ├─ Fetch News API (parallel)
  ├─ Produce Kafka streams
  ├─ Data quality checks
  ├─ Run dbt transformations
  ├─ Validate dbt tests
  ├─ Enrich data (Pandas)
  └─ End
## 📝 Transformations

### Transformation 1: Cleaning (dbt)
- Remove null values
- Validate temperature ranges (-60 to 60°C)
- Validate humidity (0-100%)
- Validate pressure (800-1200 hPa)

### Transformation 2: Aggregation (dbt)
- Daily weather summaries by city
- Min/max/average temperatures
- Articles count by source
- Content length metrics

### Transformation 3: Enrichment (Pandas)
- Calculate heat index from temp + humidity
- Add temperature categories
- Extract news quality scores
- Add weather alerts (extreme heat/cold)

## ✅ Testing

Run tests with:
```bash
pytest tests/ -v
```

## 🚢 Deployment

### Local Docker

```bash
docker-compose up -d
# All services running locally
```

### Free Cloud Deployment

The free deployment uses Streamlit Community Cloud for the dashboard, Neon
free PostgreSQL for the warehouse, and GitHub Actions for hourly ingestion.
Render's scheduled jobs require a paid plan, so Render is not used for
automation.

```bash
git push origin main
```

#### 1. Create the free PostgreSQL database

Create a database on Neon and copy its pooled connection string. It normally
starts with `postgresql://` and includes `sslmode=require`.

#### 2. Add the GitHub Actions secret

In GitHub, open **Settings → Secrets and variables → Actions**, create a
repository secret named `DATABASE_URL`, and paste the Neon connection string.

The workflow in
`.github/workflows/weather-ingestion.yml` runs once per hour and can also be
started manually with **Actions → Hourly weather ingestion → Run workflow**.

#### 3. Deploy the dashboard

Create a Streamlit Community Cloud app from this repository:

- Main file: `streamlit_app.py`
- Branch: `main`
- Secret name: `DATABASE_URL`
- Secret value: the same Neon connection string

The dashboard reads live PostgreSQL data whenever the connection is
available. If the secret is missing or the database is empty, it clearly
labels the fallback values as demo data.

## 🔗 Links

- **GitHub:** https://github.com/USERNAME/data-pipeline
- **Dashboard:** https://yourapp.onrender.com
- **Airflow:** http://localhost:8080 (local only)
- **Documentation:** [See docs/ folder]

## 👤 Author

- Name: Ben Soussia Jawhar
- Email: soussiajawhar2@gmail.com
- GitHub: @Jawhar Soussia

## 📄 License

MIT License - Feel free to use this project!

## 🙏 Acknowledgments

- Asma Karakni (Project Supervisor)
- Apache Airflow Community
- dbt Community
- Streamlit Team

---
