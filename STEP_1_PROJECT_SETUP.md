# STEP 1: Project Setup & Environment Configuration

**Status:** Foundation Phase
**Duration:** 2-3 hours
**Owner Notes:** This is critical. Get this right and everything else flows. Get this wrong and you'll debug environment issues for days.

---

## 🎯 Objective

Set up a professional, reproducible development environment using Docker and Git where your entire pipeline can run consistently across any computer.

---

## 📋 Mandatory Deliverables for This Step

- ✅ Git repository (local initialized, ready to push)
- ✅ Docker and Docker Compose installed
- ✅ Project directory structure organized
- ✅ Python virtual environment configured
- ✅ All dependencies documented in requirements.txt
- ✅ docker-compose.yml with all services defined
- ✅ .gitignore configured properly
- ✅ README.md started with architecture diagram

---

## 🔧 PHASE 1: Initialize Git Repository

### Step 1.1: Create Project Structure

Open your terminal and execute these commands exactly:

```bash
# Navigate to your project directory
cd "c:\Users\souss\OneDrive - Ministere de l'Enseignement Superieur et de la Recherche Scientifique\Desktop\DS\S2\Big Data (Asma karakni)\projet"

# Initialize git
git init

# Create directory structure
mkdir -p src/{ingestion,transformation,orchestration}
mkdir -p data/{raw,processed,staging}
mkdir -p tests
mkdir -p dbt/models/{staging,marts}
mkdir -p dbt/tests
mkdir -p airflow/{dags,plugins,logs}
mkdir -p config
mkdir -p docs

# Verify structure
ls -la
```

**Expected Output:**
```
drwxr-xr-x  data/
drwxr-xr-x  src/
drwxr-xr-x  tests/
drwxr-xr-x  dbt/
drwxr-xr-x  airflow/
drwxr-xr-x  config/
drwxr-xr-x  docs/
```

---

### Step 1.2: Create .gitignore File

Create file: `.gitignore`

```
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
venv/
ENV/
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual environments
.venv
venv

# IDEs
.vscode/
.idea/
*.swp
*.swo
*~

# Environment variables
.env
.env.local
.env.*.local

# Credentials and secrets
credentials.json
secrets.yml
.aws/
.gcp/

# Logs
logs/
*.log
airflow/logs/

# Data
data/raw/*
data/staging/*
!data/raw/.gitkeep
!data/staging/.gitkeep

# dbt
dbt/target/
dbt/dbt_packages/

# MinIO
minio_data/

# Docker
postgres_data/
kafka_data/

# OS
.DS_Store
Thumbs.db

# IDE
.vscode/settings.json
```

---

### Step 1.3: Initialize Git and Create First Commit

```bash
# Add all files
git add .

# Create first commit
git commit -m "Initial project structure setup

- Created directory structure for data pipeline
- Added .gitignore for Python/Docker



# Verify
git log --oneline
```

**Expected:**
```
d1a2b3c (HEAD -> main) Initial project structure setup
```

---

## 🐍 PHASE 2: Python Environment Setup

### Step 2.1: Create requirements.txt

Create file: `requirements.txt`

```
# Data ingestion
requests==2.31.0
httpx==0.25.0
python-dotenv==1.0.0

# Data processing
pandas==2.1.0
polars==0.19.0
duckdb==0.9.0

# Database
psycopg2-binary==2.9.9
sqlalchemy==2.0.23
alembic==1.12.0

# dbt
dbt-core==1.7.0
dbt-postgres==1.7.0

# Orchestration
apache-airflow==2.7.0
apache-airflow-providers-postgres==5.10.0
apache-airflow-providers-apache-kafka==5.7.0

# Kafka
kafka-python==2.0.2
confluent-kafka==2.3.0

# Testing
pytest==7.4.0
pytest-cov==4.1.0

# Visualization
streamlit==1.28.0
plotly==5.17.0

# Utilities
pydantic==2.5.0
pyyaml==6.0.1
```

---

### Step 2.2: Create Python Virtual Environment

```bash
# Create virtual environment
python -m venv venv

# Activate it
# On Windows:
venv\Scripts\activate
# On Mac/Linux:
source venv/bin/activate

# Verify activation (should show (venv) prefix)
python --version

# Install dependencies
pip install -r requirements.txt

# Verify installation
pip list | grep -E "pandas|airflow|streamlit"
```

**Expected Output (should show all installed):**
```
pandas           2.1.0
apache-airflow   2.7.0
streamlit        1.28.0
...
```

---

### Step 2.3: Create .env Template

Create file: `.env.example` (this is a TEMPLATE, never commit real secrets)

```bash
# PostgreSQL
POSTGRES_USER=datauser
POSTGRES_PASSWORD=your_secure_password_here
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=data_pipeline_db

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_HOST=minio
MINIO_PORT=9000

# Kafka
KAFKA_BROKER=kafka:9092
KAFKA_TOPIC_PREFIX=data-pipeline

# Airflow
AIRFLOW_HOME=/opt/airflow
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__UNIT_TEST_MODE=True

# API Keys (replace with real ones)
OPENWEATHER_API_KEY=your_api_key_here
NEWSAPI_KEY=your_api_key_here

# Application
DEBUG=True
LOG_LEVEL=INFO
```

**⚠️ IMPORTANT:** Create `.env` (actual file, NOT committed):
```bash
cp .env.example .env
# Edit .env with real values
```

---

## 🐳 PHASE 3: Docker & Docker Compose Setup

### Step 3.1: Install Docker

**Windows:**
1. Download Docker Desktop from https://www.docker.com/products/docker-desktop
2. Install it
3. Verify:
   ```bash
   docker --version
   docker-compose --version
   ```

**Expected:**
```
Docker version 24.0.0
Docker Compose version v2.22.0
```

---

### Step 3.2: Create docker-compose.yml

Create file: `docker-compose.yml`

```yaml
version: '3.8'

services:
  # PostgreSQL Database
  postgres:
    image: postgres:15-alpine
    container_name: data-pipeline-postgres
    environment:
      POSTGRES_USER: ${POSTGRES_USER:-datauser}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-datauser}
      POSTGRES_DB: ${POSTGRES_DB:-data_pipeline_db}
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./config/init.sql:/docker-entrypoint-initdb.d/init.sql
    networks:
      - data-pipeline
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER:-datauser}"]
      interval: 10s
      timeout: 5s
      retries: 5

  # MinIO (S3-compatible object storage)
  minio:
    image: minio/minio:latest
    container_name: data-pipeline-minio
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER:-minioadmin}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD:-minioadmin}
    ports:
      - "9000:9000"
      - "9001:9001"  # Console
    volumes:
      - minio_data:/minio_data
    networks:
      - data-pipeline
    command: server /minio_data --console-address ":9001"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 30s
      timeout: 20s
      retries: 3

  # Apache Kafka
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: data-pipeline-kafka
    depends_on:
      zookeeper:
        condition: service_healthy
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    ports:
      - "9092:9092"
    networks:
      - data-pipeline
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions.sh", "--bootstrap-server", "localhost:9092"]
      interval: 10s
      timeout: 5s
      retries: 5

  # Zookeeper (required by Kafka)
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: data-pipeline-zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"
    networks:
      - data-pipeline
    healthcheck:
      test: ["CMD", "echo", "ruok | nc localhost 2181"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  postgres_data:
  minio_data:

networks:
  data-pipeline:
    driver: bridge
```

---

### Step 3.3: Create init.sql for PostgreSQL

Create file: `config/init.sql`

```sql
-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- Create audit tables
CREATE TABLE IF NOT EXISTS public.dbt_tests (
    id SERIAL PRIMARY KEY,
    test_name VARCHAR(255),
    status VARCHAR(50),
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA raw TO datauser;
GRANT ALL PRIVILEGES ON SCHEMA staging TO datauser;
GRANT ALL PRIVILEGES ON SCHEMA marts TO datauser;
```

---

### Step 3.4: Test Docker Compose

```bash
# Start all services
docker-compose up -d

# Wait 30 seconds for services to start
sleep 30

# Check status
docker-compose ps

# Check logs
docker-compose logs postgres
docker-compose logs minio
docker-compose logs kafka

# Stop services (keep data)
docker-compose down
```

**Expected Output from `docker-compose ps`:**
```
NAME                              STATUS
data-pipeline-postgres            Up (healthy)
data-pipeline-minio               Up (healthy)
data-pipeline-kafka               Up (healthy)
data-pipeline-zookeeper           Up (healthy)
```

---

## 📁 PHASE 4: Project File Structure Verification

Create placeholder files to indicate directory purposes:

```bash
# Create .gitkeep files so empty directories are tracked
touch data/raw/.gitkeep
touch data/processed/.gitkeep
touch data/staging/.gitkeep
touch airflow/logs/.gitkeep
touch dbt/models/staging/.gitkeep
touch dbt/models/marts/.gitkeep
touch tests/.gitkeep

# Verify structure
tree -L 2  # or "ls -R" on Windows
```

**Final Structure Should Look Like:**
```
projet/
├── src/
│   ├── ingestion/
│   ├── transformation/
│   └── orchestration/
├── data/
│   ├── raw/
│   ├── processed/
│   └── staging/
├── tests/
├── dbt/
│   └── models/
│       ├── staging/
│       └── marts/
├── airflow/
│   ├── dags/
│   ├── plugins/
│   └── logs/
├── config/
│   └── init.sql
├── docs/
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

---

## 📖 PHASE 5: Initialize README with Architecture

Create file: `README.md`

```markdown
# Data Pipeline End-to-End Project

## 🎯 Project Overview

[To be completed - describe your specific use case]

## 📊 Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                  DATA PIPELINE ARCHITECTURE                 │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  Data Sources                                               │
│  ├── API 1 (e.g., OpenWeather)                             │
│  └── API 2 (e.g., NewsAPI)                                 │
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

[Your contact information]

## 📄 License

MIT License
```

---

## ✅ PHASE 6: Commit and Verify

```bash
# Activate virtual environment if not already activated
source venv/bin/activate  # or venv\Scripts\activate on Windows

# Add all setup files
git add .
git add -u

# Commit
git commit -m "Configure project environment and Docker setup

- Added Python virtual environment with all dependencies
- Created docker-compose.yml with PostgreSQL, MinIO, Kafka
- Set up project directory structure
- Added .gitignore and .env template
- Initialized README with architecture diagram

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"

# View commit
git log --oneline -5

# Check status
git status
```

---

## 🔍 Verification Checklist

Before moving to Step 2, verify ALL of these:

- [x] Git repository initialized with first commit
- [x] Virtual environment created and activated
- [x] All dependencies installed (`pip list` shows pandas, airflow, streamlit, etc.)
- [x] Docker installed and running
- [x] docker-compose up -d starts all 4 services successfully
- [x] docker-compose ps shows all services as "Up"
- [x] PostgreSQL accessible: `docker-compose exec postgres psql -U datauser -d data_pipeline_db`
- [x] MinIO console accessible at http://localhost:9001
- [x] Kafka broker running: `docker-compose exec kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092`
- [x] .gitignore prevents secrets from being committed
- [x] README.md created with architecture diagram
- [x] Project structure matches expected layout

---

## 🎓 Key Takeaways

1. **Reproducibility:** Docker makes your environment identical across machines
2. **Isolation:** Virtual env keeps project dependencies separate
3. **Version Control:** Git tracks every change
4. **Documentation:** README and architecture diagram are mandatory
5. **Secret Management:** Never commit .env with real credentials

---

## 🆘 Troubleshooting

### Docker Services Won't Start
```bash
# Check logs
docker-compose logs

# Full restart
docker-compose down -v
docker-compose up -d
```

### Python Import Errors
```bash
# Ensure virtual env is activated
source venv/bin/activate
pip install -r requirements.txt --upgrade
```

### Git Issues
```bash
# Check current status
git status

# View commits
git log --oneline

# Undo last commit (if needed)
git reset --soft HEAD~1
```

---

## 📋 Next Step

Once this step is complete, move to: **STEP 2: Data Ingestion (Batch)**

You now have a professional foundation. Everything we build next will live in this structure!
