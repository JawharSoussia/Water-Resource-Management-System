@echo off
REM Quick startup script for Docker Compose on Windows

setlocal enabledelayedexpansion

echo.
echo ==========================================
echo Starting Airflow with Docker Compose
echo ==========================================
echo.

REM Check if Docker is running
docker info >nul 2>&1
if errorlevel 1 (
    echo.
    echo ERROR: Docker is not running. Please start Docker Desktop.
    echo.
    pause
    exit /b 1
)

echo OK - Docker is running
echo.

REM Check if .env exists
if not exist .env (
    echo Creating .env from template...
    copy .env.docker .env
    echo OK - .env created
)

echo.
echo Building Docker images...
docker-compose build --quiet
if errorlevel 1 (
    echo ERROR: Build failed
    pause
    exit /b 1
)
echo OK - Build complete
echo.

echo Starting services...
docker-compose up -d
if errorlevel 1 (
    echo ERROR: Failed to start services
    pause
    exit /b 1
)
echo OK - Services started
echo.

echo.
echo ==========================================
echo OK - All services started!
echo ==========================================
echo.
echo Service URLs:
echo   Airflow UI:    http://localhost:8080
echo   MinIO Console: http://localhost:9001
echo   PostgreSQL:    localhost:5432
echo   Kafka:         localhost:9092
echo.
echo Default Credentials:
echo   Airflow - admin / admin123
echo   MinIO   - minioadmin / minioadmin
echo.
echo Next steps:
echo   1. Open http://localhost:8080 in your browser
echo   2. Log in with admin / admin123
echo   3. Check DAGs are loaded under 'DAGs' tab
echo   4. Trigger 'data_pipeline_end_to_end' DAG
echo.
echo To view logs:
echo   docker-compose logs -f airflow-scheduler
echo.
echo To stop services:
echo   docker-compose stop
echo.
pause
