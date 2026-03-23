# Windows Consumer Lag Monitoring Guide


---

## Option 1: Python Continuous Monitoring (Recommended)

### Usage:
```bash
# Check every 5 seconds (default)
python monitor_lag.py

# Check every 10 seconds
python monitor_lag.py 10

# Check every 2 seconds
python monitor_lag.py 2
```

### Features:
✅ Cross-platform (Windows/Mac/Linux)
✅ Auto-refresh with screen clear
✅ Shows iteration count
✅ Easy to stop: Press CTRL+C
✅ No external dependencies needed

### Example Output:
```
==========================================================================================
[MONITOR] Consumer Lag Monitoring (Check #1)
[TIME] Updated at: 2026-03-20 22:45:30
==========================================================================================

[LAG] CONSUMER LAG MONITORING DATA
==========================================================================================

[OK] Total lag records in database: 2
[METRICS] LATEST LAG METRICS (by topic and consumer group):
...
```

---

## Option 2: PowerShell Loop

### Create a batch file: `monitor.bat`
```batch
@echo off
setlocal enabledelayedexpansion

:loop
cls
echo Monitoring Consumer Lag - Press CTRL+C to stop
echo Time: %date% %time%
echo.
python src/database/check_consumer_lag.py
echo.
echo Waiting 5 seconds...
timeout /t 5 /nobreak
goto loop
```

### Run:
```bash
monitor.bat
```

---

## Option 3: PowerShell Loop

### Run directly in PowerShell:
```powershell
while ($true) {
    Clear-Host
    Write-Host "Monitoring Consumer Lag - Press CTRL+C to stop"
    Write-Host "Time: $(Get-Date)"
    Write-Host ""
    python src/database/check_consumer_lag.py
    Write-Host ""
    Start-Sleep -Seconds 5
}
```

---

## Option 4: One-Time Check (Simplest)

If you just want to check lag without continuous monitoring:

```bash
python src/database/check_consumer_lag.py
```

---

## Production Monitoring Setup

For production environments, combine multiple verification methods:

### Terminal 1: Start Producer
```bash
python src/ingestion/kafka_weather_producer.py
```

### Terminal 2: Start Consumer
```bash
python src/ingestion/kafka_consumer.py
```

### Terminal 3: Monitor Lag (Continuous)
```bash
python monitor_lag.py 5
```

### Terminal 4: Monitor Data (One-time)
```bash
python src/database/verify_streaming_data.py
```

---

## Docker Monitoring

View container logs in real-time:

```bash
# Kafka logs
docker-compose logs -f kafka --tail 50

# PostgreSQL logs
docker-compose logs -f postgres --tail 50

# All services
docker-compose logs -f --tail 50
```

---

## Recommended Monitoring Strategy

1. **Every 5 seconds:** Consumer lag status
2. **Every 30 seconds:** Data verification (record counts)
3. **Every minute:** Docker container health
4. **Manual checks:** When alerts trigger

---

## Quick Reference

| Command | Purpose |
|---------|---------|
| `python monitor_lag.py` | Continuous lag monitoring (5s intervals) |
| `python monitor_lag.py 10` | Continuous lag monitoring (10s intervals) |
| `python src/database/check_consumer_lag.py` | Single lag check |
| `python src/database/verify_streaming_data.py` | Verify data in database |
| `python src/ingestion/kafka_consumer.py` | Run consumer |
| `python src/ingestion/kafka_weather_producer.py` | Run weather producer |
| `docker-compose logs -f kafka --tail 50` | Stream Kafka logs |

---

## Troubleshooting Monitoring

If monitoring script fails:

1. **Check Python is in PATH:**
   ```bash
   python --version
   ```

2. **Verify database connection:**
   ```bash
   python src/database/check_consumer_lag.py
   ```

3. **Check Kafka connectivity:**
   ```bash
   python -c "from kafka import KafkaProducer; print('Kafka OK')"
   ```

4. **Verify PostgreSQL is running:**
   ```bash
   docker-compose ps
   ```

---

## Example: Full Monitoring Dashboard

Create `full_monitor.bat`:

```batch
@echo off
setlocal enabledelayedexpansion

:loop
cls
echo ============================================================================
echo KAFKA STREAMING PIPELINE DASHBOARD - %date% %time%
echo ============================================================================
echo.
echo --- CONSUMER LAG STATUS ---
python src/database/check_consumer_lag.py
echo.
echo --- DATA VERIFICATION ---
python src/database/verify_streaming_data.py
echo.
echo Refreshing in 10 seconds...
timeout /t 10 /nobreak
goto loop
```

Run: `full_monitor.bat`

---

## Notes

- All scripts use `localhost` and default PostgreSQL credentials
- Consumer lag will show 0 if all messages are processed
- Data verification shows record counts from all streaming tables
- Timestamps are in UTC format
- Use CTRL+C to stop any monitoring script
