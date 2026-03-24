# Docker Quick Start Guide

## ⚡ 5-Minute Setup

### Step 1: Install Docker Desktop
- Download: https://www.docker.com/products/docker-desktop
- Install & restart your computer

### Step 2: Start Services (Easy!)

**Option A - Windows (Easiest)**
```bash
# Double-click this file:
start-docker.bat
```

**Option B - Command Line (Either OS)**
```bash
cd "c:\Users\souss\OneDrive - Ministere de l'Enseignement Superieur et de la Recherche Scientifique\Desktop\DS\S2\Big Data (Asma karakni)\projet"

# Start everything
docker-compose up -d
```

### Step 3: Wait 30-60 seconds

Services starting:
- PostgreSQL (data storage)
- Redis (caching)
- Zookeeper (Kafka coordination)
- Kafka (streaming)
- MinIO (object storage)
- Airflow Webserver (UI)
- Airflow Scheduler (executes DAGs)
- Airflow Triggerer (async tasks)

### Step 4: Access Airflow UI

Open in your browser: **http://localhost:8080**

Login:
- Username: `admin`
- Password: `admin123`

## 🎯 Common Commands

```bash
# Check status
docker-compose ps

# View logs
docker-compose logs -f airflow-scheduler

# Stop everything
docker-compose stop

# Start again
docker-compose up -d

# Clean everything (removes data)
docker-compose down -v

# Run tests inside container
docker-compose exec airflow-webserver pytest tests/test_dags.py -v

# List DAGs
docker-compose exec airflow-webserver airflow dags list

# Trigger a DAG
docker-compose exec airflow-webserver airflow dags trigger data_pipeline_end_to_end
```

## 📊 Access Points

| Service | URL/Host | Credentials |
|---------|----------|-------------|
| **Airflow UI** | http://localhost:8080 | admin / admin123 |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin |
| **PostgreSQL** | localhost:5432 | airflow / airflow |
| **Kafka** | localhost:9092 | - |
| **Redis** | localhost:6379 | - |

## ⚠️ Troubleshooting

**Services won't start?**
```bash
# Check Docker is running
docker info

# View error logs
docker-compose logs

# Rebuild from scratch
docker-compose down -v
docker-compose build --no-cache
docker-compose up -d
```

**Port already in use?**
- Edit `.env` file to change ports
- Restart: `docker-compose restart`

**Out of disk space?**
```bash
docker system prune -a
```

## 📚 Full Documentation

See `DOCKER_SETUP.md` for complete guide including:
- Production considerations
- Advanced troubleshooting
- Custom configuration
- Performance tuning

---

**Next Time You Use This:**
1. Just run `start-docker.bat` or `docker-compose up -d`
2. Wait 30 seconds
3. Open http://localhost:8080
4. Done! ✨
