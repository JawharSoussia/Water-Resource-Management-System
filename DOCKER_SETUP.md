# Docker Setup for Apache Airflow

This guide explains how to run the entire data pipeline with Airflow using Docker Compose instead of native Windows installation (which has compatibility issues).

## Prerequisites

1. **Docker Desktop** - Download from https://www.docker.com/products/docker-desktop
   - Version 4.0+
   - Includes Docker Engine and Docker Compose

2. **Verify Installation**
   ```bash
   docker --version
   docker-compose --version
   ```

## Quick Start

### 1. Prepare Environment

```bash
# Navigate to project root
cd "c:\Users\souss\OneDrive - Ministere de l'Enseignement Superieur et de la Recherche Scientifique\Desktop\DS\S2\Big Data (Asma karakni)\projet"

# Copy environment template
cp .env.docker .env

# Update requirements.txt if needed
# (Dockerfile will use requirements-airflow.txt which is pre-configured)
```

### 2. Build and Start All Services

```bash
# Build Airflow image (one-time)
docker-compose build

# Start all services in background
docker-compose up -d

# Or start with logs visible
docker-compose up
```

### 3. Verify Services are Running

```bash
# Check all containers
docker-compose ps

# Expected output:
# NAME                    STATUS
# airflow-postgres        Up (healthy)
# airflow-redis           Up (healthy)
# airflow-zookeeper       Up (healthy)
# airflow-kafka           Up (healthy)
# airflow-minio           Up (healthy)
# airflow-webserver       Up
# airflow-scheduler       Up
# airflow-triggerer       Up
```

### 4. Access Services

**Airflow UI:**
- URL: http://localhost:8080
- Username: admin
- Password: admin123

**MinIO Console:**
- URL: http://localhost:9001
- Username: minioadmin
- Password: minioadmin

**PostgreSQL:**
- Host: localhost:5432
- Database: airflow
- User: airflow
- Password: airflow

**Kafka:**
- Bootstrap Server: localhost:9092

## Common Tasks

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver

# Last 100 lines
docker-compose logs --tail 100 airflow-scheduler
```

### Execute Commands Inside Containers

```bash
# List DAGs
docker-compose exec airflow-webserver airflow dags list

# Trigger a DAG
docker-compose exec airflow-webserver airflow dags trigger data_pipeline_end_to_end

# Run tests
docker-compose exec airflow-webserver pytest tests/test_dags.py -v

# Access PostgreSQL
docker-compose exec postgres psql -U airflow -d airflow
```

### Monitor DAG Execution

```bash
# List recent runs
docker-compose exec airflow-webserver airflow dags list-runs -d data_pipeline_end_to_end

# View task logs
docker-compose exec airflow-webserver airflow tasks logs data_pipeline_end_to_end setup_pipeline
```

## Troubleshooting

### Containers won't start

```bash
# Check Docker daemon
docker info

# Check Docker logs
docker-compose logs

# Verify ports are not in use
# On Windows PowerShell:
Get-NetTcpConnection -LocalPort 8080  # or 5432, 9092, etc.
```

### Permission Denied Errors

```bash
# If you get permission errors, restart Docker
docker-compose restart

# Or rebuild from scratch
docker-compose down -v
docker-compose build --no-cache
docker-compose up -d
```

### Out of Disk Space

```bash
# Remove unused containers/images
docker system prune

# Remove all volumes (WARNING: loses data)
docker volume prune
```

### Port Already in Use

Edit `.env` file and change port numbers:
```
AIRFLOW_WEBSERVER_PORT=8888  # instead of 8080
POSTGRES_PORT=5433           # instead of 5432
KAFKA_PORT=9093              # instead of 9092
```

## Stop Services

```bash
# Stop all running containers (keeps volumes)
docker-compose stop

# Remove containers but keep volumes
docker-compose down

# Remove everything including volumes (WARNING: deletes data)
docker-compose down -v
```

## Start Existing Services (After Stop)

```bash
# Restart
docker-compose up -d

# Check status
docker-compose ps
```

## Production Considerations

The current setup is development-focused. For production:

1. Use **external PostgreSQL** instead of containerized
2. Use **CeleryExecutor** instead of LocalExecutor
3. Add **load balancing** for Airflow webserver
4. Enable **HTTPS** and proper authentication
5. Configure **persistent volumes** for data
6. Add **monitoring** (Prometheus, Grafana)
7. Implement **backup** strategies

## File Structure

```
project/
├── docker-compose.yml          # Main Docker configuration
├── Dockerfile.airflow          # Custom Airflow image
├── requirements-airflow.txt    # Python dependencies
├── .env.docker                 # Environment template
├── .env                        # Your local environment (git-ignored)
├── airflow/
│   ├── dags/                   # Your DAG files
│   ├── plugins/                # Custom Airflow plugins
│   └── logs/                   # Container will mount logs here
├── src/                        # Python source code
├── dbt/                        # dbt models and tests
└── tests/                      # Test files
```

## Next Steps

1. Create DAG files in `airflow/dags/`
2. Trigger DAGs from Airflow UI
3. Monitor execution in UI
4. Check logs as needed
5. Iterate and improve

## Support

For Docker issues: https://docs.docker.com/
For Airflow issues: https://airflow.apache.org/docs/
For our project: Check STEP_5_ORCHESTRATION.md
