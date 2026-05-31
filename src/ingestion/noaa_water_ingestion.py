"""NOAA Global Water API data ingestion for AQUA system."""

import os
import logging
import requests
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import uuid
from dotenv import load_dotenv
import yaml

from .water_models import (
    WaterStation, WaterMeasurement, WaterDataBatch,
    WaterStationType, WaterParameterType, GeoCoordinate,
    WATER_PARAMETER_RULES
)

load_dotenv()

logger = logging.getLogger(__name__)

class NOAAWaterIngestion:
    """Ingest water data from NOAA Global Water API."""

    def __init__(self):
        """Initialize NOAA water ingestion."""
        self.base_url = os.getenv(
            "NOAA_API_BASE_URL",
            "https://www.ncei.noaa.gov/api/v2"
        )
        self.timeout = int(os.getenv("NOAA_TIMEOUT", "30"))
        self.region = os.getenv("NOAA_REGION", "MENA")
        self.datasets = [
            ds.strip()
            for ds in os.getenv("NOAA_DATASETS", "nclimgrid").split(",")
            if ds.strip()
        ]

        # Load water sources config
        self.config = self._load_config()
        self.water_stations = self._initialize_stations()

        logger.info(f"NOAA Water Ingestion initialized with {len(self.water_stations)} stations")

    def validate_water_data(self, data: Dict[str, Any]) -> bool:
        """Backward-compatible NOAA payload validation used in unit tests."""
        if not isinstance(data, dict):
            return False

        results = data.get("results")
        if results is None:
            return False

        if not isinstance(results, list):
            return False

        if not results:
            return True

        required_keys = {"dataset", "latitude", "longitude"}
        return all(required_keys.issubset(item.keys()) for item in results if isinstance(item, dict))

    def fetch_water_data(self, dataset: str) -> Optional[Dict[str, Any]]:
        """Backward-compatible single-call fetch interface used in unit tests."""
        try:
            endpoint = f"{self.base_url}/{dataset}"
            response = requests.get(endpoint, timeout=self.timeout)
            response.raise_for_status()
            payload = response.json()

            if not isinstance(payload, dict):
                return None

            payload["_source"] = "noaa_global"
            payload["_region"] = self.region
            payload["_dataset"] = dataset
            payload["_timestamp"] = datetime.utcnow().isoformat()
            return payload
        except Exception as e:
            logger.error(f"Failed to fetch NOAA dataset {dataset}: {e}")
            return None

    def _load_config(self) -> Dict[str, Any]:
        """Load water configuration."""
        config_path = "config/water_sources_config.yaml"
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        return {}

    def _initialize_stations(self) -> List[WaterStation]:
        """Initialize water stations from config."""
        stations = []

        if 'water_systems' not in self.config:
            logger.warning("No water systems configured")
            return stations

        for country, system in self.config['water_systems'].items():
            for station_config in system.get('stations', []):
                station = WaterStation(
                    station_id=f"{country.upper()}_{station_config['name'].replace(' ', '_')}",
                    station_name=station_config['name'],
                    station_type=WaterStationType(station_config['type']),
                    location=GeoCoordinate(
                        latitude=station_config['latitude'],
                        longitude=station_config['longitude'],
                        region=country
                    ),
                    country_code=country.upper(),
                    agency="NOAA",
                    active=True
                )
                stations.append(station)

        return stations

    def fetch_water_levels(self) -> List[WaterDataBatch]:
        """Fetch current water levels for all stations."""
        batches = []

        for station in self.water_stations:
            try:
                batch = self._fetch_station_data(station)
                if batch:
                    batches.append(batch)
                    logger.info(f"✓ Successfully fetched data for {station.station_name}")
            except Exception as e:
                logger.error(f"✗ Failed to fetch data for {station.station_name}: {e}")
                continue

        return batches

    def _fetch_station_data(self, station: WaterStation) -> Optional[WaterDataBatch]:
        """Fetch data for a single station."""
        try:
            # Build request for NOAA gridded climate data
            endpoint = f"{self.base_url}/climgrid"

            params = {
                'lat': station.location.latitude,
                'lon': station.location.longitude,
                'bbox': f'{station.location.latitude-1},{station.location.longitude-1},'
                        f'{station.location.latitude+1},{station.location.longitude+1}',
                'format': 'json',
                'dataTypes': 'pcp,tmax,tmin'  # Precipitation, temperature
            }

            response = requests.get(endpoint, params=params, timeout=self.timeout)
            response.raise_for_status()

            data = response.json()
            measurements = self._parse_measurements(station, data)

            batch = WaterDataBatch(
                batch_id=str(uuid.uuid4()),
                station=station,
                measurements=measurements,
                batch_timestamp=datetime.utcnow(),
                source="NOAA",
                total_records=len(measurements)
            )

            return batch

        except requests.RequestException as e:
            logger.error(f"Request failed for {station.station_name}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error processing station {station.station_name}: {e}")
            return None

    def _parse_measurements(self, station: WaterStation, data: Dict) -> List[WaterMeasurement]:
        """Parse NOAA response into measurements."""
        measurements = []

        if 'results' not in data:
            return measurements

        for result in data.get('results', []):
            try:
                # Precipitation measurement
                if 'pcp' in result:
                    measurement = WaterMeasurement(
                        measurement_id=str(uuid.uuid4()),
                        station_id=station.station_id,
                        station_name=station.station_name,
                        parameter=WaterParameterType.PRECIPITATION,
                        value=float(result['pcp']),
                        unit='mm',
                        timestamp=datetime.fromisoformat(result['date']),
                        data_source="NOAA",
                        is_validated=False
                    )
                    measurements.append(measurement)

                # Temperature measurements
                if 'tmax' in result:
                    temp_measurement = WaterMeasurement(
                        measurement_id=str(uuid.uuid4()),
                        station_id=station.station_id,
                        station_name=station.station_name,
                        parameter=WaterParameterType.TEMPERATURE,
                        value=float(result['tmax']),
                        unit='°C',
                        timestamp=datetime.fromisoformat(result['date']),
                        data_source="NOAA",
                        is_validated=False
                    )
                    measurements.append(temp_measurement)

            except (KeyError, ValueError) as e:
                logger.warning(f"Failed to parse measurement for {station.station_name}: {e}")
                continue

        return measurements

    def validate_measurements(self, measurements: List[WaterMeasurement]) -> tuple:
        """Validate water measurements against rules."""
        valid = []
        invalid = []

        for measurement in measurements:
            try:
                rules = WATER_PARAMETER_RULES.get(measurement.parameter)
                if not rules:
                    logger.warning(f"No validation rules for {measurement.parameter}")
                    valid.append(measurement)
                    continue

                if not (rules['min'] <= measurement.value <= rules['max']):
                    measurement.notes = (
                        f"Value {measurement.value} {measurement.unit} out of range "
                        f"[{rules['min']}, {rules['max']}]"
                    )
                    invalid.append(measurement)
                else:
                    measurement.is_validated = True
                    valid.append(measurement)

            except Exception as e:
                logger.error(f"Validation error for {measurement.measurement_id}: {e}")
                invalid.append(measurement)

        return valid, invalid


def main():
    """Main ingestion function."""
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
    )

    ingestion = NOAAWaterIngestion()

    # Fetch water data
    logger.info("Starting NOAA water data ingestion...")
    batches = ingestion.fetch_water_levels()

    # Validate all measurements
    all_valid = []
    all_invalid = []

    for batch in batches:
        valid, invalid = ingestion.validate_measurements(batch.measurements)
        all_valid.extend(valid)
        all_invalid.extend(invalid)
        batch.failed_records = len(invalid)

    # Log results
    logger.info(f"✓ Ingestion complete: {len(all_valid)} valid, {len(all_invalid)} invalid")

    # Return for MinIO storage
    return batches


if __name__ == "__main__":
    main()