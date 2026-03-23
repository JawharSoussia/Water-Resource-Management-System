# Quick Start Guide - Kafka Streaming Pipeline

## Prerequisites

✅ Docker running (`docker-compose ps` shows all containers)
✅ Python 3.10+ installed
✅ Virtual environment activated (if using venv)

---

## Quick Start: 2-Step Setup

### Step 1: Initialize Database Tables
Run once to create all streaming tables:

```bash
python src/database/init_streaming_tables.py
```

**Expected Output:**
```
[OK] Connected to PostgreSQL at localhost:data_pipeline_db
[OK] Stream tables created successfully!
[OK] Verified 4 tables created
```

---

### Step 2: Run the Complete Pipeline

Open **4 separate terminal windows** (or use terminal multiplexer):

#### Terminal 1: Weather Data Producer
```bash
python src/ingestion/kafka_weather_producer.py
```
**What it does:** Fetches weather data from OpenWeather API and sends to Kafka every 2 seconds

#### Terminal 2: Weather Data Consumer
```bash
python src/ingestion/kafka_consumer.py
```
**What it does:** Reads weather messages from Kafka and stores in PostgreSQL

#### Terminal 3: Consumer Lag Monitor (Windows)
```bash
python monitor_lag.py 5
```
OR (using batch file):
```bash
monitor_lag.bat
```
**What it does:** Displays consumer lag metrics every 5 seconds

#### Terminal 4: Data Verification (Optional)
```bash
python src/database/verify_streaming_data.py
```
**What it does:** Shows current record counts and sample data

---

## Expected Output Example

### Terminal 1 (Producer):
```
[OK] Sending weather data for 4 locations...
[1s] [OK] Sent weather data for Tunis (total: 1)
[3s] [OK] Sent weather data for Sfax (total: 2)
[5s] [OK] Sent weather data for London (total: 3)
```

### Terminal 2 (Consumer):
```
2026-03-20 22:50:10,123 - src.ingestion.kafka_consumer - INFO - Starting consumer...
[OK] Processed weather event for Tunis
[OK] Processed weather event for Sfax
[OK] Processed weather event for London
```

### Terminal 3 (Monitor):
```
==========================================================================================
[LAG] CONSUMER LAG MONITORING DATA - Check #1
[OK] Total lag records in database: 6

[METRICS] LATEST LAG METRICS:
Partition 0: Cur Offset=3, End Offset=3, Lag=0 [OK]
Partition 1: Cur Offset=3, End Offset=3, Lag=0 [OK]
Total lag: 0 messages

Next refresh in 5 seconds...
```

### Terminal 4 (Verification):
```
[OK] Total records in weather_stream: 6
[OK] Latest records:
  - Tunis: 22.5°C, humidity 65%, timestamp 2026-03-20 22:50:10
  - London: 8.6°C, humidity 77%, timestamp 2026-03-20 22:50:08
  - Sfax: 25.5°C, humidity 72%, timestamp 2026-03-20 22:50:06
```

---

## Production Workflow (Continuous Running)

1. **Morning Start:**
   - Open 4 terminals
   - Start Producer → Consumer → Monitor
   - Check initial data flow

2. **During Day:**
   - Producer runs continuously
   - Consumer processes messages
   - Monitor displays metrics
   - Lag stays at 0 (messages processed immediately)

3. **Monitor Health:**
   - Use Terminal 3 to watch lag metrics
   - Use Terminal 4 to verify data storage
   - Check Docker logs for errors: `docker-compose logs -f`

4. **End of Day:**
   - Stop Producer (CTRL+C in Terminal 1)
   - Let Consumer finish remaining messages
   - Stop Consumer (CTRL+C in Terminal 2)
   - Stop Monitor (CTRL+C in Terminal 3)

---

## Configuration

### Change Check Interval
Monitor lag every N seconds:

```bash
# Every 10 seconds
python monitor_lag.py 10

# Every 2 seconds
python monitor_lag.py 2

# Default (5 seconds) - no argument needed
python monitor_lag.py
```

### Change Weather Locations
Edit `.env` file:
```
OPENWEATHER_LOCATIONS=Tunis,Sfax,Kairouan,Sidi_Bouzid
```

### Change Kafka Broker
Edit `.env` file:
```
KAFKA_BROKER=127.0.0.1:9092
```

---

## Troubleshooting Quick Start

### Error: "Connection refused"
**Cause:** Docker containers not running
**Fix:**
```bash
docker-compose up -d
docker-compose ps  # Verify all containers are running
```

### Error: "No such file or directory"
**Cause:** Running from wrong directory
**Fix:**
```bash
cd "path/to/DS/S2/Big Data (Asma karakni)/projet"
```

### Error: "ModuleNotFoundError: kafka"
**Cause:** Dependencies not installed
**Fix:**
```bash
pip install -r requirements.txt
```

### Consumer not processing messages
**Cause:** Producer not sending data
**Fix:** Check Terminal 1 output, verify producer is running

### Lag not updating
**Cause:** No messages in queue
**Fix:** Verify producer is running and sending data to Kafka

---

## Quick Commands Reference

```bash
# Initialize
python src/database/init_streaming_tables.py

# Run Components
python src/ingestion/kafka_weather_producer.py      # Terminal 1
python src/ingestion/kafka_consumer.py              # Terminal 2
python monitor_lag.py 5                             # Terminal 3
python src/database/verify_streaming_data.py        # Terminal 4

# Monitor Docker
docker-compose ps                # Container status
docker-compose logs -f kafka     # Kafka logs
docker-compose logs -f postgres  # Database logs
docker-compose restart           # Restart all

# Database
python src/database/check_consumer_lag.py           # Lag metrics
python src/database/verify_streaming_data.py        # Data verification

# Tests
pytest tests/test_streaming.py -v                   # Run tests
```

---

## Expected Data Flow

```
[Weather API]
     ↓
[Kafka Producer] → sends to → [Kafka Broker]
                                    ↓
[Kafka Consumer] ← reads from ← [Kafka Topic: aqua-realtime-weather]
     ↓
[PostgreSQL] ← inserts into ← [staging.weather_stream]
     ↓
[Consumer Group] ← tracks ← [monitoring.consumer_lag]
```

---

## Next Steps

After successfully running the pipeline:

1. **Add Water Data Producer**
   - `python src/ingestion/kafka_realtime_producer.py`
   - Creates water stream data alongside weather

2. **Add Alert Processing**
   - Configure government alerts topic
   - Update consumer to handle alerts

3. **Add Monitoring Dashboard**
   - Create Streamlit dashboard (requires `streamlit install`)
   - Real-time visualization of metrics

4. **Scale to Production**
   - Deploy consumers to multiple servers
   - Use load balancer for Kafka topics
   - Add data retention policies
   - Set up alerting for lag > threshold

---

## Support

For issues or questions:
- Check WINDOWS_MONITORING_GUIDE.md for Windows-specific help
- Review E2E_TEST_RESULTS.md for test details
- Check STEP_3_STREAMING_KAFKA.md for architecture details
- View Docker logs: `docker-compose logs [service_name]`

---

Good luck with your streaming pipeline! 🚀
