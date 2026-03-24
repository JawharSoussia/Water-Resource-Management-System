
"""Water data models and schemas."""

from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum
import json


class WaterStationType(Enum):
    """Types of water measurement stations."""
    RIVER = "river"
    LAKE = "lake"
    GROUNDWATER = "groundwater"
    SPRING = "spring"
    RESERVOIR = "reservoir"
    SALT_LAKE = "salt_lake"
    AGRICULTURAL = "agricultural"
    MONITORING_STATION = "monitoring_station"


class WaterParameterType(Enum):
    """Water quality and quantity parameters."""
    WATER_LEVEL = "water_level"  # meters
    DISCHARGE = "discharge"  # cubic meters per second
    PRECIPITATION = "precipitation"  # millimeters
    TEMPERATURE = "temperature"  # celsius
    TURBIDITY = "turbidity"  # NTU (Nephelometric Turbidity Units)
    PH = "ph"  # pH level
    DISSOLVED_OXYGEN = "dissolved_oxygen"  # mg/L
    CONDUCTIVITY = "conductivity"  # µS/cm
    SALINITY = "salinity"  # ppt (parts per thousand)
    NITRATE = "nitrate"  # mg/L


@dataclass
class GeoCoordinate:
    """Geographic location."""
    latitude: float
    longitude: float
    elevation_meters: Optional[float] = None
    region: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def validate(self) -> bool:
        """Validate geographic coordinates."""
        if not (-90 <= self.latitude <= 90):
            raise ValueError(f"Invalid latitude: {self.latitude}")
        if not (-180 <= self.longitude <= 180):
            raise ValueError(f"Invalid longitude: {self.longitude}")
        return True


@dataclass
class WaterStation:
    """Water measurement station metadata."""
    station_id: str
    station_name: str
    station_type: WaterStationType
    location: GeoCoordinate

    # Optional metadata
    country_code: Optional[str] = None
    agency: Optional[str] = None  # NOAA, USGS, etc.
    established_date: Optional[str] = None
    active: bool = True

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['station_type'] = self.station_type.value
        data['location'] = self.location.to_dict()
        return data


@dataclass
class WaterMeasurement:
    """Single water parameter measurement."""
    measurement_id: str
    station_id: str
    station_name: str
    parameter: WaterParameterType
    value: float
    unit: str
    timestamp: datetime

    # Quality indicators
    quality_code: Optional[str] = None  # A (approved), P (provisional), etc.
    data_source: Optional[str] = "unknown"  # NOAA, USGS, OpenWeather
    is_validated: bool = False
    notes: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['parameter'] = self.parameter.value
        data['timestamp'] = self.timestamp.isoformat()
        return data


@dataclass
class WaterDataBatch:
    """Batch of water measurements for a station."""
    batch_id: str
    station: WaterStation
    measurements: List[WaterMeasurement]
    batch_timestamp: datetime
    source: str  # NOAA, USGS, OpenWeather
    total_records: int
    failed_records: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'batch_id': self.batch_id,
            'station': self.station.to_dict(),
            'measurements': [m.to_dict() for m in self.measurements],
            'batch_timestamp': self.batch_timestamp.isoformat(),
            'source': self.source,
            'total_records': self.total_records,
            'failed_records': self.failed_records
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


# Validation rules
WATER_PARAMETER_RULES = {
    WaterParameterType.WATER_LEVEL: {
        'min': -100,      # meters below sea level
        'max': 5000,      # meters above sea level
        'unit': 'meters'
    },
    WaterParameterType.DISCHARGE: {
        'min': 0,
        'max': 100000,    # cubic meters per second
        'unit': 'm³/s'
    },
    WaterParameterType.PRECIPITATION: {
        'min': 0,
        'max': 500,       # mm per day
        'unit': 'mm'
    },
    WaterParameterType.TEMPERATURE: {
        'min': -50,
        'max': 60,        # celsius
        'unit': '°C'
    },
    WaterParameterType.PH: {
        'min': 0,
        'max': 14,
        'unit': 'pH'
    },
    WaterParameterType.DISSOLVED_OXYGEN: {
        'min': 0,
        'max': 20,        # mg/L
        'unit': 'mg/L'
    },
    WaterParameterType.SALINITY: {
        'min': 0,
        'max': 40,        # parts per thousand
        'unit': 'ppt'
    }
}