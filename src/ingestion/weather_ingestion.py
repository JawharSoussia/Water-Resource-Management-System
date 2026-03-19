"""OpenWeather API data ingestion."""

import os
import time
import logging
import requests
from datetime import datetime
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from .minio_client import MinIOClient

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
        self.cities = os.getenv("OPENWEATHER_LOCATIONS", "").split(",")
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