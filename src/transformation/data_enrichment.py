"""Data enrichment with Pandas - complex business logic."""

import pandas as pd
import numpy as np
from datetime import datetime
import logging
from sqlalchemy import create_engine
import os

logger = logging.getLogger(__name__)


class DataEnrichment:
    """Enrichment transformations for cleaned data."""

    def __init__(self):
        """Initialize database connection."""
        db_user = os.getenv("POSTGRES_USER", "datauser")
        db_password = os.getenv("POSTGRES_PASSWORD", "datauser")
        db_host = os.getenv("POSTGRES_HOST", "localhost")
        db_name = os.getenv("POSTGRES_DB", "data_pipeline_db")

        self.engine = create_engine(
            f"postgresql://{db_user}:{db_password}@{db_host}/{db_name}"
        )

    def enrich_weather_data(self):
        """
        Enrichment Transformation 3:
        Add calculated fields and classifications to weather data.
        """
        logger.info("Starting weather data enrichment")

        # Load data
        df = pd.read_sql("SELECT * FROM staging.stg_weather", self.engine)

        # Add calculated columns
        # 1. Calculate heat index (simplified)
        df['heat_index_celsius'] = self._calculate_heat_index(
            df['temperature_celsius'],
            df['humidity_percent']
        )

        # 2. Add temperature category
        df['temperature_category'] = pd.cut(
            df['temperature_celsius'],
            bins=[-np.inf, -10, 0, 10, 20, 30, np.inf],
            labels=['Extreme Cold', 'Very Cold', 'Cold', 'Cool', 'Warm', 'Hot']
        )

        # 3. Add weather alerts
        df['has_extreme_heat'] = (df['temperature_celsius'] > 35).astype(int)
        df['has_extreme_cold'] = (df['temperature_celsius'] < -15).astype(int)
        df['has_high_wind'] = (df['wind_speed_ms'] > 10).astype(int)
        df['has_low_visibility'] = (df['visibility_meters'] < 1000).astype(int)

        # 4. Add decade information
        df['temperature_decade'] = (df['temperature_celsius'] // 10 * 10).astype(int)

        # 5. Add humidity classification
        df['humidity_level'] = pd.cut(
            df['humidity_percent'],
            bins=[0, 30, 60, 100],
            labels=['Dry', 'Moderate', 'High']
        )

        # Save enriched data
        df.to_sql(
            'weather_enriched',
            self.engine,
            schema='marts',
            if_exists='replace',
            index=False
        )

        logger.info(f"Enriched {len(df)} weather records")
        return df

    def enrich_news_data(self):
        """
        Enrichment with text analysis and categorization.
        """
        logger.info("Starting news data enrichment")

        df = pd.read_sql("SELECT * FROM staging.stg_news", self.engine)

        # Add calculated columns
        # 1. Classify by title length
        df['title_length_category'] = pd.cut(
            df['title'].str.len(),
            bins=[0, 50, 100, 150, np.inf],
            labels=['Short', 'Medium', 'Long', 'Very Long']
        )

        # 2. Calculate content completeness score
        df['has_description'] = (~df['description'].isna()).astype(int)
        df['has_content'] = (~df['content'].isna()).astype(int)
        df['completeness_score'] = (
            df['has_description'] * 0.5 + df['has_content'] * 0.5
        )

        # 3. Days since publication
        df['days_since_published'] = (
            (datetime.utcnow() - df['published_at']).dt.days
        )

        # 4. Freshness indicator
        df['is_fresh'] = (df['days_since_published'] <= 7).astype(int)

        # 5. Content quality score
        df['quality_score'] = (
            (df['content_length'] > 500).astype(float) * 0.5 +
            df['completeness_score'] * 0.5
        )

        # Save enriched data
        df.to_sql(
            'news_enriched',
            self.engine,
            schema='marts',
            if_exists='replace',
            index=False
        )

        logger.info(f"Enriched {len(df)} news records")
        return df

    @staticmethod
    def _calculate_heat_index(temperature: pd.Series, humidity: pd.Series) -> pd.Series:
        """
        Calculate heat index (Simplified formula).

        Args:
            temperature: Temperature in Celsius
            humidity: Humidity percentage

        Returns:
            Heat index in Celsius
        """
        # Convert to Fahrenheit for calculation
        temp_f = (temperature * 9 / 5) + 32

        # Heat index formula (simplified)
        hi = (
            -42.379 +
            (2.04901523 * temp_f) +
            (10.14333127 * humidity) -
            (0.22475541 * temp_f * humidity) -
            (0.00683783 * temp_f ** 2) -
            (0.05481717 * humidity ** 2) +
            (0.00122874 * temp_f ** 2 * humidity) +
            (0.00085282 * temp_f * humidity ** 2) -
            (0.00000199 * temp_f ** 2 * humidity ** 2)
        )

        # Convert back to Celsius
        return (hi - 32) * 5 / 9

    def close(self):
        """Close database connection."""
        self.engine.dispose()


def main():
    """Main entry point."""
    logging.basicConfig(level=logging.INFO)

    enrichment = DataEnrichment()

    try:
        enrichment.enrich_weather_data()
        enrichment.enrich_news_data()
        logger.info("All enrichments complete!")
    finally:
        enrichment.close()


if __name__ == "__main__":
    main()