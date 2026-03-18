# STEP 2: Data Ingestion - Batch API Integration

**Status:** Ingestion Phase - Part 1
**Duration:** 3-4 hours
**Owner Notes:** This is where your data pipeline comes to life. Get comfortable with APIs, error handling, and data validation. You'll ingest using these patterns for the rest of the project.

---

## 🎯 Objective

Build robust Python scripts that automatically fetch data from 2 distinct API sources, validate the data, and store it in MinIO (raw data lake) for later transformation.

---

## 📋 Mandatory Deliverables for This Step

- ✅ 2 working API data sources configured
- ✅ Python ingestion scripts with error handling
- ✅ Data validation logic
- ✅ Structured logging
- ✅ Retry mechanism for failed requests
- ✅ MinIO bucket setup and upload functionality
- ✅ Unit tests for ingestion functions
- ✅ README documenting data sources

---

## 🔍 PHASE 1: Choose Your Data Sources

### Decision Point: What data will you ingest?

**Choose TWO from these categories:**

#### Option A: Weather Data
- **Source 1:** OpenWeather API (free tier: 1,000 calls/day)
  - Endpoint: `https://api.openweathermap.org/data/2.5/weather`
  - Get API key: https://openweathermap.org/api

- **Source 2:** Weather.gov (US) or NOAA (no API key needed)
  - Endpoint: `https://api.weather.gov/points/<lat>,<lon>`

#### Option B: Finance/Stocks
- **Source 1:** Alpha Vantage (stock data, free tier)
  - Endpoint: `https://www.alphavantage.co/query`
  - Get API key: https://www.alphavantage.co/

- **Source 2:** CoinGecko Crypto (no API key needed)
  - Endpoint: `https://api.coingecko.com/api/v3`

#### Option C: News & Content
- **Source 1:** NewsAPI (free tier: 100 req/day)
  - Endpoint: `https://newsapi.org/v2/everything`
  - Get API key: https://newsapi.org/

- **Source 2:** Guardian Open Platform
  - Endpoint: `https://open-platform.theguardian.com/search`

#### Option D: Sports Data
- **Source 1:** ESPN API (unofficial, no key needed)
  - Endpoint: `https://site.api.espn.com/`

- **Source 2:** TheSportsDB (free, no key)
  - Endpoint: `https://www.thesportsdb.com/api/`

---

### Decision: I Choose...

**For this Water Management guide, I'll use:**
1. **USGS Water Data API** - Real streamflow & water levels (FREE, no API key needed)
2. **OpenWeather API** - Real-time weather for evaporation prediction (FREE tier)

**These are AQUA water system data sources!**

---

## 📝 PHASE 2: Set Up API Keys

### Step 2.1: Register for APIs

**USGS Water Data (NO API KEY NEEDED):**
- Go to https://waterservices.usgs.gov/rest/DailyValues.html
- No registration required
- All data is public and free

**OpenWeather:**
1. Go to https://openweathermap.org/api
2. Click "Sign Up"
3. Create free account
4. Navigate to API keys tab
5. Copy your API key

---

### Step 2.2: Add API Keys to .env

Edit `.env` file:

```bash
# Water System Data Sources
# USGS - No API key needed
USGS_STATIONS=02193500,02196500  # Add your station IDs here
USGS_BASE_URL=https://waterservices.usgs.gov/nwis

# OpenWeather - Get free key from openweathermap.org
OPENWEATHER_API_KEY=your_actual_key_here
OPENWEATHER_LOCATIONS=Sfax,Tunis,Kairouan,Sidi_Bouzid

# MinIO Configuration
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET_RAW=aqua-raw-data

# Ingestion Settings
INGESTION_LOG_LEVEL=INFO
MAX_RETRIES=3
RETRY_DELAY_SECONDS=5
```

---

## 📦 PHASE 3: Install MinIO Client

### Step 3.1: Create S3/MinIO Connection Helper

Create file: `src/ingestion/minio_client.py`

```python
"""MinIO client for uploading raw data to object storage."""

import os
import json
from datetime import datetime
from pathlib import Path
from minio import Minio
from minio.error import S3Error
import logging

logger = logging.getLogger(__name__)


class MinIOClient:
    """Handles uploads to MinIO object storage."""

    def __init__(self):
        """Initialize MinIO client from environment variables."""
        self.endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        self.access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        self.bucket = os.getenv("MINIO_BUCKET_RAW", "raw-data")

        # Parse endpoint (remove http://)
        endpoint = self.endpoint.replace("http://", "").replace("https://", "")

        self.client = Minio(
            endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=False,  # True if using HTTPS
        )

        logger.info(f"MinIO client initialized: {endpoint}")

    def ensure_bucket_exists(self):
        """Create bucket if it doesn't exist."""
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
                logger.info(f"Created bucket: {self.bucket}")
            else:
                logger.info(f"Bucket exists: {self.bucket}")
        except S3Error as e:
            logger.error(f"Error creating bucket: {e}")
            raise

    def upload_data(self, source_name: str, data: dict) -> str:
        """
        Upload JSON data to MinIO.

        Args:
            source_name: API source name (e.g., "openweather")
            data: Dictionary containing the API response

        Returns:
            S3 object path where data was uploaded
        """
        try:
            # Create object path: raw-data/source_name/YYYY/MM/DD/HH-MM-SS.json
            now = datetime.utcnow()
            object_path = (
                f"raw/{source_name}/"
                f"{now.year:04d}/{now.month:02d}/{now.day:02d}/"
                f"{now.hour:02d}-{now.minute:02d}-{now.second:02d}.json"
            )

            # Convert data to JSON
            data_json = json.dumps(data, indent=2).encode("utf-8")

            # Upload to MinIO
            self.client.put_object(
                self.bucket,
                object_path,
                data=io.BytesIO(data_json),
                length=len(data_json),
                content_type="application/json",
            )

            logger.info(f"Uploaded to MinIO: s3://{self.bucket}/{object_path}")
            return object_path

        except S3Error as e:
            logger.error(f"Error uploading to MinIO: {e}")
            raise

    def list_uploaded_files(self, source_name: str) -> list:
        """List all files uploaded from a specific source."""
        try:
            objects = self.client.list_objects(
                self.bucket, prefix=f"raw/{source_name}/", recursive=True
            )
            return [obj.object_name for obj in objects]
        except S3Error as e:
            logger.error(f"Error listing objects: {e}")
            return []


# Required import
import io
```

---

## 🌡️ PHASE 4: Build Weather Data Ingestion

### Step 4.1: Create Weather Ingestion Script

Create file: `src/ingestion/weather_ingestion.py`

```python
"""OpenWeather API data ingestion."""

import os
import time
import logging
import requests
from datetime import datetime
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from minio_client import MinIOClient

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class WeatherIngestion:
    """Handles weather data ingestion from OpenWeather API."""

    def __init__(self):
        """Initialize weather ingestion client."""
        self.api_key = os.getenv("OPENWEATHER_API_KEY")
        self.cities = os.getenv("OPENWEATHER_CITIES", "").split(",")
        self.base_url = "https://api.openweathermap.org/data/2.5/weather"
        self.max_retries = int(os.getenv("MAX_RETRIES", 3))
        self.retry_delay = int(os.getenv("RETRY_DELAY_SECONDS", 5))
        self.minio = MinIOClient()

        if not self.api_key:
            raise ValueError("OPENWEATHER_API_KEY not set in .env")

        logger.info(f"Weather ingestion initialized for cities: {self.cities}")

    def fetch_weather(self, city: str) -> Optional[Dict[str, Any]]:
        """
        Fetch weather data for a city with retry logic.

        Args:
            city: City name to fetch weather for

        Returns:
            Weather data dict or None if failed after retries
        """
        params = {
            "q": city.strip(),
            "appid": self.api_key,
            "units": "metric",  # Use Celsius
        }

        for attempt in range(self.max_retries):
            try:
                logger.info(f"Fetching weather for {city} (attempt {attempt + 1}/{self.max_retries})")

                response = requests.get(
                    self.base_url,
                    params=params,
                    timeout=10
                )
                response.raise_for_status()

                data = response.json()

                # Add metadata
                data["_ingested_at"] = datetime.utcnow().isoformat()
                data["_source"] = "openweather_api"
                data["_city"] = city

                logger.info(f"Successfully fetched weather for {city}")
                return data

            except requests.exceptions.Timeout:
                logger.warning(f"Timeout fetching {city} (attempt {attempt + 1})")
            except requests.exceptions.HTTPError as e:
                logger.warning(f"HTTP error: {e.response.status_code} (attempt {attempt + 1})")
            except Exception as e:
                logger.warning(f"Error fetching weather for {city}: {e} (attempt {attempt + 1})")

            # Wait before retrying
            if attempt < self.max_retries - 1:
                logger.info(f"Waiting {self.retry_delay}s before retry...")
                time.sleep(self.retry_delay)

        logger.error(f"Failed to fetch weather for {city} after {self.max_retries} attempts")
        return None

    def validate_weather_data(self, data: Dict[str, Any]) -> bool:
        """
        Validate weather data has required fields.

        Args:
            data: Weather data dictionary

        Returns:
            True if valid, False otherwise
        """
        required_fields = ["main", "temp", "humidity", "pressure", "visibility"]

        try:
            main = data.get("main", {})
            has_fields = all(field in main for field in ["temp", "humidity", "pressure"])
            has_visibility = "visibility" in data

            if not (has_fields and has_visibility):
                logger.warning(f"Missing required fields in weather data for {data.get('name')}")
                return False

            return True
        except Exception as e:
            logger.error(f"Error validating weather data: {e}")
            return False

    def ingest_all_cities(self) -> Dict[str, Any]:
        """
        Fetch weather for all configured cities.

        Returns:
            Summary of ingestion results
        """
        logger.info(f"Starting weather ingestion for {len(self.cities)} cities")

        results = {
            "total_cities": len(self.cities),
            "successful": 0,
            "failed": 0,
            "validated": 0,
            "uploaded": 0,
            "timestamp": datetime.utcnow().isoformat(),
            "details": {}
        }

        # Ensure MinIO bucket exists
        self.minio.ensure_bucket_exists()

        for city in self.cities:
            try:
                # Fetch data
                weather_data = self.fetch_weather(city)

                if not weather_data:
                    results["failed"] += 1
                    results["details"][city] = "Failed to fetch"
                    continue

                results["successful"] += 1

                # Validate data
                if not self.validate_weather_data(weather_data):
                    results["details"][city] = "Validation failed"
                    continue

                results["validated"] += 1

                # Upload to MinIO
                self.minio.upload_data("weather", weather_data)
                results["uploaded"] += 1
                results["details"][city] = "Success"

            except Exception as e:
                logger.error(f"Error processing {city}: {e}")
                results["failed"] += 1
                results["details"][city] = str(e)

        logger.info(f"Weather ingestion complete: {results['uploaded']}/{results['total_cities']} uploaded")
        return results


def main():
    """Main entry point for weather ingestion."""
    try:
        ingestion = WeatherIngestion()
        results = ingestion.ingest_all_cities()

        # Log summary
        logger.info(f"\n=== INGESTION SUMMARY ===")
        logger.info(f"Total cities: {results['total_cities']}")
        logger.info(f"Successful: {results['successful']}")
        logger.info(f"Failed: {results['failed']}")
        logger.info(f"Validated: {results['validated']}")
        logger.info(f"Uploaded: {results['uploaded']}")

        return results
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
```

---

## 💧 PHASE 5: Build Water Data Ingestion (USGS)

### Step 5.1: Create USGS Water Data Ingestion Script

Create file: `src/ingestion/usgs_water_ingestion.py`

```python
"""USGS Real-Time Water Data ingestion."""

import os
import time
import logging
import requests
from datetime import datetime
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from minio_client import MinIOClient

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class USGSWaterIngestion:
    """Handles water data ingestion from USGS National Water Information System."""

    def __init__(self):
        """Initialize USGS water ingestion client."""
        self.stations = os.getenv("USGS_STATIONS", "").split(",")
        self.base_url = "https://waterservices.usgs.gov/nwis/instantaneous"
        self.max_retries = int(os.getenv("MAX_RETRIES", 3))
        self.retry_delay = int(os.getenv("RETRY_DELAY_SECONDS", 5))
        self.minio = MinIOClient()

        if not self.stations or self.stations == ['']:
            raise ValueError("USGS_STATIONS not set in .env. Get station IDs from https://waterdata.usgs.gov/")

        logger.info(f"USGS water ingestion initialized for stations: {self.stations}")

    def fetch_water_data(self, station: str) -> Optional[Dict[str, Any]]:
        """
        Fetch water data for a station with retry logic.

        Args:
            station: USGS station ID (e.g., "02193500")

        Returns:
            Water data dict or None if failed after retries
        """
        params = {
            "sites": station.strip(),
            "parameters": "00060,00065",  # Streamflow + stage
            "format": "json"
        }

        for attempt in range(self.max_retries):
            try:
                logger.info(f"Fetching water data for station {station} (attempt {attempt + 1}/{self.max_retries})")

                response = requests.get(
                    self.base_url,
                    params=params,
                    timeout=10
                )
                response.raise_for_status()

                data = response.json()

                # Add metadata
                data["_ingested_at"] = datetime.utcnow().isoformat()
                data["_source"] = "usgs_nwis"
                data["_station"] = station

                logger.info(f"Successfully fetched water data for station {station}")
                return data

            except requests.exceptions.Timeout:
                logger.warning(f"Timeout fetching station {station} (attempt {attempt + 1})")
            except requests.exceptions.HTTPError as e:
                logger.warning(f"HTTP error: {e.response.status_code} (attempt {attempt + 1})")
            except Exception as e:
                logger.warning(f"Error fetching water data for station {station}: {e} (attempt {attempt + 1})")

            # Wait before retrying
            if attempt < self.max_retries - 1:
                logger.info(f"Waiting {self.retry_delay}s before retry...")
                time.sleep(self.retry_delay)

        logger.error(f"Failed to fetch water data for station {station} after {self.max_retries} attempts")
        return None

    def validate_water_data(self, data: Dict[str, Any]) -> bool:
        """
        Validate water data has required fields.

        Args:
            data: Water data dictionary

        Returns:
            True if valid, False otherwise
        """
        try:
            if "value" not in data:
                logger.warning(f"Missing 'value' field in water data")
                return False

            time_series = data.get("value", {}).get("timeSeries", [])

            if not time_series:
                logger.warning(f"Empty timeSeries in water data")
                return False

            # Check first time series has data
            ts = time_series[0]
            if "sourceInfo" not in ts or "values" not in ts:
                logger.warning(f"Missing sourceInfo or values in timeSeries")
                return False

            logger.info(f"Water data validation passed")
            return True

        except Exception as e:
            logger.error(f"Error validating water data: {e}")
            return False

    def ingest_all_stations(self) -> Dict[str, Any]:
        """
        Fetch water data for all configured stations.

        Returns:
            Summary of ingestion results
        """
        logger.info(f"Starting USGS water ingestion for {len(self.stations)} stations")

        results = {
            "total_stations": len(self.stations),
            "successful": 0,
            "failed": 0,
            "validated": 0,
            "uploaded": 0,
            "timestamp": datetime.utcnow().isoformat(),
            "details": {}
        }

        # Ensure MinIO bucket exists
        self.minio.ensure_bucket_exists()

        for station in self.stations:
            try:
                # Fetch data
                water_data = self.fetch_water_data(station)

                if not water_data:
                    results["failed"] += 1
                    results["details"][station] = "Failed to fetch"
                    continue

                results["successful"] += 1

                # Validate data
                if not self.validate_water_data(water_data):
                    results["details"][station] = "Validation failed"
                    continue

                results["validated"] += 1

                # Upload to MinIO
                self.minio.upload_data("usgs_water", water_data)
                results["uploaded"] += 1
                results["details"][station] = "Success"

            except Exception as e:
                logger.error(f"Error processing station {station}: {e}")
                results["failed"] += 1
                results["details"][station] = str(e)

        logger.info(f"Water ingestion complete: {results['uploaded']}/{results['total_stations']} uploaded")
        return results


def main():
    """Main entry point for USGS water ingestion."""
    try:
        ingestion = USGSWaterIngestion()
        results = ingestion.ingest_all_stations()

        # Log summary
        logger.info(f"\n=== USGS WATER INGESTION SUMMARY ===")
        logger.info(f"Total stations: {results['total_stations']}")
        logger.info(f"Successful: {results['successful']}")
        logger.info(f"Failed: {results['failed']}")
        logger.info(f"Validated: {results['validated']}")
        logger.info(f"Uploaded: {results['uploaded']}")

        return results
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
```

---

## ✅ PHASE 6: Test Ingestion Scripts

### Step 6.1: Manual Testing

```bash
# Activate virtual environment
source venv/bin/activate

# Start Docker services if not already running
docker-compose up -d

# Wait for services to be ready
sleep 10

# Test weather ingestion
python src/ingestion/weather_ingestion.py

# Test USGS water ingestion
python src/ingestion/usgs_water_ingestion.py
```

**Expected Output:**
```
2024-01-15 10:30:45 - usgs_water_ingestion - INFO - USGS water ingestion initialized for stations: 02193500,02196500
2024-01-15 10:30:45 - usgs_water_ingestion - INFO - Starting USGS water ingestion for 2 stations
2024-01-15 10:30:46 - usgs_water_ingestion - INFO - Fetching water data for station 02193500 (attempt 1/1)
2024-01-15 10:30:47 - usgs_water_ingestion - INFO - Successfully fetched water data for station 02193500
...
=== USGS WATER INGESTION SUMMARY ===
Total stations: 2
Successful: 2
Failed: 0
Validated: 2
Uploaded: 2
```

---

### Step 6.2: Verify MinIO Upload

```bash
# List files uploaded to MinIO
docker-compose exec minio mc ls minio-local/raw-data/raw/ --recursive

# Access MinIO console
# Go to http://localhost:9001
# Login with minioadmin / minioadmin
# Browse to raw-data bucket to see uploaded files
```

---

## 🧪 PHASE 7: Write Unit Tests

Create file: `tests/test_ingestion.py`

```python
"""Unit tests for data ingestion modules."""

import pytest
import os
from datetime import datetime
from unittest.mock import Mock, patch
from src.ingestion.weather_ingestion import WeatherIngestion
from src.ingestion.usgs_water_ingestion import USGSWaterIngestion


class TestWeatherIngestion:
    """Tests for weather ingestion."""

    @pytest.fixture
    def weather_ingestion(self):
        """Create WeatherIngestion instance for testing."""
        return WeatherIngestion()

    def test_initialization(self, weather_ingestion):
        """Test WeatherIngestion initializes correctly."""
        assert weather_ingestion.api_key is not None
        assert len(weather_ingestion.cities) > 0

    def test_validate_valid_weather_data(self, weather_ingestion):
        """Test validation of valid weather data."""
        valid_data = {
            "name": "London",
            "main": {
                "temp": 15.5,
                "humidity": 72,
                "pressure": 1013
            },
            "visibility": 10000
        }
        assert weather_ingestion.validate_weather_data(valid_data) is True

    def test_validate_invalid_weather_data(self, weather_ingestion):
        """Test validation of invalid weather data."""
        invalid_data = {
            "name": "London",
            "main": {}  # Missing required fields
        }
        assert weather_ingestion.validate_weather_data(invalid_data) is False

    @patch('requests.get')
    def test_fetch_weather_success(self, mock_get, weather_ingestion):
        """Test successful weather fetch."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "name": "London",
            "main": {
                "temp": 15.5,
                "humidity": 72,
                "pressure": 1013
            },
            "visibility": 10000
        }
        mock_get.return_value = mock_response

        result = weather_ingestion.fetch_weather("London")
        assert result is not None
        assert result["_source"] == "openweather_api"

    @patch('requests.get')
    def test_fetch_weather_failure(self, mock_get, weather_ingestion):
        """Test weather fetch with API error."""
        mock_get.side_effect = Exception("API Error")

        result = weather_ingestion.fetch_weather("London")
        assert result is None


class TestUSGSWaterIngestion:
    """Tests for USGS water data ingestion."""

    @pytest.fixture
    def water_ingestion(self):
        """Create USGSWaterIngestion instance for testing."""
        return USGSWaterIngestion()

    def test_initialization(self, water_ingestion):
        """Test USGSWaterIngestion initializes correctly."""
        assert water_ingestion.stations is not None
        assert len(water_ingestion.stations) > 0

    def test_validate_valid_water_data(self, water_ingestion):
        """Test validation of valid water data."""
        valid_data = {
            "value": {
                "timeSeries": [
                    {
                        "sourceInfo": {"siteName": "Test Station"},
                        "values": [{"value": [{"dateTime": "2024-01-15T00:00:00.000", "value": "1000"}]}]
                    }
                ]
            }
        }
        assert water_ingestion.validate_water_data(valid_data) is True

    def test_validate_invalid_water_data(self, water_ingestion):
        """Test validation of invalid water data."""
        invalid_data = {
            "value": {
                "timeSeries": []  # Empty timeSeries
            }
        }
        assert water_ingestion.validate_water_data(invalid_data) is False

    @patch('requests.get')
    def test_fetch_water_success(self, mock_get, water_ingestion):
        """Test successful water data fetch."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "value": {
                "timeSeries": [
                    {
                        "sourceInfo": {"siteName": "Test Station"},
                        "values": [{"value": [{"dateTime": "2024-01-15T00:00:00.000", "value": "1000"}]}]
                    }
                ]
            }
        }
        mock_get.return_value = mock_response

        result = water_ingestion.fetch_water_data("02193500")
        assert result is not None
        assert result["_source"] == "usgs_nwis"

    @patch('requests.get')
    def test_fetch_water_failure(self, mock_get, water_ingestion):
        """Test water fetch with API error."""
        mock_get.side_effect = Exception("API Error")

        result = water_ingestion.fetch_water_data("02193500")
        assert result is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
```

---

### Step 7.1: Run Tests

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src/ingestion

# Run specific test file
pytest tests/test_ingestion.py::TestWeatherIngestion -v
```

**Expected Output:**
```
tests/test_ingestion.py::TestWeatherIngestion::test_initialization PASSED
tests/test_ingestion.py::TestWeatherIngestion::test_validate_valid_weather_data PASSED
tests/test_ingestion.py::TestWeatherIngestion::test_validate_invalid_weather_data PASSED
tests/test_ingestion.py::TestNewsIngestion::test_initialization PASSED
...

========== 8 passed in 0.45s ==========
```

---

## 📊 PHASE 8: Create Ingestion Documentation

Create file: `docs/ingestion.md`

```markdown
# Data Ingestion Documentation

## Overview

This document describes how data is ingested from external APIs into the MinIO data lake.

## Configured Data Sources

### 1. OpenWeather API
- **URL:** https://api.openweathermap.org/data/2.5/weather
- **Frequency:** Every 6 hours (via Airflow)
- **Cities:** Configurable via OPENWEATHER_CITIES in .env
- **Fields:** Temperature, humidity, pressure, visibility, wind speed
- **Authentication:** API key in OPENWEATHER_API_KEY

### 2. USGS Water Data (No API key needed!)
- **URL:** https://waterservices.usgs.gov/nwis/instantaneous
- **Frequency:** Every 6 hours (via Airflow)
- **Stations:** Configurable via USGS_STATIONS in .env
- **Fields:** Streamflow (CFS), water stage (feet), quality flags
- **Authentication:** None! Public USGS data

## Ingestion Flow

```
[API Endpoint]
      ↓
[Python Script]
      ├─ Fetch data with retry logic (max 3 attempts)
      ├─ Validate required fields
      ├─ Add ingestion metadata
      └─ Upload to MinIO
      ↓
[MinIO: aqua-raw-data bucket]
```

## Error Handling

All ingestion scripts implement:
- **Retry Logic:** Up to 3 attempts with 5-second delays
- **Logging:** Structured logs with timestamps and levels
- **Validation:** Required fields checked before upload
- **Timeout:** 10-second request timeout per API call

## MonitoringChecks

Before moving to the transformation phase, verify:
1. All data successfully uploaded to MinIO
2. No validation errors in logs
3. All tests passing (pytest tests/)
4. API keys configured in .env
5. Docker services running (docker-compose ps)

## Future Enhancements

- Add data schema validation (pydantic)
- Implement data quality metrics
- Add alerting for failed ingestions
- Parallel API requests for multiple cities
```

---

## 📋 Commit Your Work

```bash
# Add all ingestion code and tests
git add src/ingestion/ tests/test_ingestion.py docs/ingestion.md

# Commit
git commit -m "Implement batch data ingestion from APIs

- Created OpenWeather API ingestion with retry logic
- Created NewsAPI ingestion with data validation
- Added MinIO client for uploading raw data
- Implemented comprehensive unit tests (8 tests)
- Added structured logging and error handling
- Documented ingestion sources and flow

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"

# Verify
git log --oneline -3
```

---

## ✅ Verification Checklist

Before moving to Step 3, verify ALL of these:

- [ ] Python scripts run without errors
- [ ] Both API data sources successfully fetch data
- [ ] Data validates before upload
- [ ] MinIO bucket has uploaded files visible in console
- [ ] All 8+ tests passing (pytest)
- [ ] Retry logic tested and working
- [ ] Logging shows clear information
- [ ] .env file has both API keys configured
- [ ] MinIO client successfully creates connections
- [ ] Documentation complete
- [ ] Git commit created with all changes

---

## 🆘 Troubleshooting

### "OPENWEATHER_API_KEY not set"
```bash
# Check .env file exists and has key
cat .env | grep OPENWEATHER_API_KEY

# If empty, get free API key from:
# https://openweathermap.org/api
```

### "Connection refused to MinIO"
```bash
# Ensure MinIO is running
docker-compose ps minio

# If not running, start services
docker-compose up -d minio
```

### "HTTP 401 Unauthorized"
```bash
# Invalid API key
# Get new key from API provider
# Update .env file
```

### Test failures
```bash
# Install test dependencies
pip install pytest pytest-cov

# Run with verbose output
pytest tests/ -vv -s
```

---

## 🎓 Key Learnings

1. **Error Handling:** Always implement retry logic for external APIs
2. **Logging:** Structured logs help debug production issues
3. **Validation:** Never trust external data; always validate
4. **Testing:** Unit tests catch bugs early
5. **Configuration:** Use environment variables for secrets

---

## 📋 Next Steps

Once this step is complete, move to: **STEP 3: Real-Time Streaming with Kafka**

You now have reliable data ingestion from multiple sources!
