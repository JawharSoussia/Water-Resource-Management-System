"""Tests for water data ingestion."""

import pytest
from datetime import datetime
from src.ingestion.water_models import (
    WaterStation, WaterMeasurement, WaterDataBatch,
    WaterStationType, WaterParameterType, GeoCoordinate,
    WATER_PARAMETER_RULES
)


class TestGeoCoordinate:
    """Test geographic coordinate validation."""

    def test_valid_coordinates(self):
        """Test valid coordinates."""
        coord = GeoCoordinate(latitude=35.7942, longitude=10.1671)
        assert coord.validate() == True

    def test_invalid_latitude(self):
        """Test invalid latitude."""
        with pytest.raises(ValueError):
            coord = GeoCoordinate(latitude=95, longitude=10.1671)
            coord.validate()

    def test_invalid_longitude(self):
        """Test invalid longitude."""
        with pytest.raises(ValueError):
            coord = GeoCoordinate(latitude=35, longitude=200)
            coord.validate()


class TestWaterStation:
    """Test water station models."""

    def test_station_creation(self):
        """Test creating a water station."""
        coord = GeoCoordinate(latitude=35.7942, longitude=10.1671)
        station = WaterStation(
            station_id="TN_MEDJERDA",
            station_name="Oued Medjerda",
            station_type=WaterStationType.RIVER,
            location=coord,
            country_code="TN",
            agency="NOAA"
        )

        assert station.station_id == "TN_MEDJERDA"
        assert station.active == True


class TestWaterMeasurement:
    """Test water measurement models."""

    def test_measurement_creation(self):
        """Test creating a measurement."""
        measurement = WaterMeasurement(
            measurement_id="M001",
            station_id="TN_MEDJERDA",
            station_name="Oued Medjerda",
            parameter=WaterParameterType.WATER_LEVEL,
            value=45.2,
            unit="meters",
            timestamp=datetime.utcnow()
        )

        assert measurement.value == 45.2
        assert measurement.is_validated == False

    def test_measurement_validation(self):
        """Test measurement validation rules."""
        # Valid water level
        valid = WaterMeasurement(
            measurement_id="M001",
            station_id="TN_MEDJERDA",
            station_name="Oued Medjerda",
            parameter=WaterParameterType.WATER_LEVEL,
            value=100,  # Within [- 100, 5000]
            unit="meters",
            timestamp=datetime.utcnow()
        )

        rules = WATER_PARAMETER_RULES[WaterParameterType.WATER_LEVEL]
        assert rules['min'] <= valid.value <= rules['max']

        # Invalid water level (too high)
        invalid = WaterMeasurement(
            measurement_id="M002",
            station_id="TN_MEDJERDA",
            station_name="Oued Medjerda",
            parameter=WaterParameterType.WATER_LEVEL,
            value=10000,  # Out of range
            unit="meters",
            timestamp=datetime.utcnow()
        )

        assert not (rules['min'] <= invalid.value <= rules['max'])


class TestWaterDataBatch:
    """Test water data batch models."""

    def test_batch_creation(self):
        """Test creating a water data batch."""
        coord = GeoCoordinate(latitude=35.7942, longitude=10.1671)
        station = WaterStation(
            station_id="TN_MEDJERDA",
            station_name="Oued Medjerda",
            station_type=WaterStationType.RIVER,
            location=coord
        )

        measurement = WaterMeasurement(
            measurement_id="M001",
            station_id="TN_MEDJERDA",
            station_name="Oued Medjerda",
            parameter=WaterParameterType.WATER_LEVEL,
            value=45.2,
            unit="meters",
            timestamp=datetime.utcnow()
        )

        batch = WaterDataBatch(
            batch_id="B001",
            station=station,
            measurements=[measurement],
            batch_timestamp=datetime.utcnow(),
            source="NOAA",
            total_records=1
        )

        assert batch.total_records == 1
        assert len(batch.measurements) == 1

    def test_batch_to_json(self):
        """Test batch to JSON conversion."""
        coord = GeoCoordinate(latitude=35.7942, longitude=10.1671)
        station = WaterStation(
            station_id="TN_MEDJERDA",
            station_name="Oued Medjerda",
            station_type=WaterStationType.RIVER,
            location=coord
        )

        batch = WaterDataBatch(
            batch_id="B001",
            station=station,
            measurements=[],
            batch_timestamp=datetime.utcnow(),
            source="NOAA",
            total_records=0
        )

        json_str = batch.to_json()
        assert isinstance(json_str, str)
        assert "batch_id" in json_str
        assert "TN_MEDJERDA" in json_str


if __name__ == "__main__":
    pytest.main([__file__, "-v"])