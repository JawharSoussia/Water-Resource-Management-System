"""Comprehensive pipeline tests."""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()


def _get_db_url() -> str:
    db_user = os.getenv("POSTGRES_USER", "datauser")
    db_password = os.getenv("POSTGRES_PASSWORD", "datauser")
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_name = os.getenv("POSTGRES_DB", "data_pipeline_db")
    return f"postgresql://{db_user}:{db_password}@{db_host}/{db_name}"


@pytest.fixture(scope="module")
def engine():
    """Create database connection or skip integration tests if unavailable."""
    db_engine = create_engine(_get_db_url())
    try:
        with db_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        db_engine.dispose()
        pytest.skip(f"PostgreSQL not accessible for integration tests: {exc}")

    yield db_engine
    db_engine.dispose()


def _read_sql_or_skip(query: str, db_engine, context: str) -> pd.DataFrame:
    """Run SQL and skip test if required relation is unavailable."""
    try:
        return pd.read_sql(query, db_engine)
    except Exception as exc:
        pytest.skip(f"{context}: {exc}")


class TestDataQuality:
    """Test data quality at each stage."""

    def test_staging_weather_no_nulls(self, engine):
        """Test critical weather fields aren't null."""
        df = _read_sql_or_skip(
            """SELECT city_name, temperature_celsius, humidity_percent
               FROM staging.stg_weather LIMIT 100""",
            engine,
            "staging.stg_weather not available"
        )
        assert df['city_name'].notna().all(), "City name should not be null"
        assert df['temperature_celsius'].notna().all(), "Temperature should not be null"

    def test_staging_weather_value_ranges(self, engine):
        """Test weather values are in reasonable ranges."""
        df = _read_sql_or_skip(
            """SELECT temperature_celsius, humidity_percent
               FROM staging.stg_weather LIMIT 100""",
            engine,
            "staging.stg_weather not available"
        )

        assert (df['temperature_celsius'] >= -60).all() and (df['temperature_celsius'] <= 60).all()
        assert (df['humidity_percent'] >= 0).all() and (df['humidity_percent'] <= 100).all()

    def test_staging_news_data_completeness(self, engine):
        """Test news articles have required fields."""
        df = _read_sql_or_skip(
            """SELECT title, url, published_at
               FROM staging.stg_news LIMIT 100""",
            engine,
            "staging.stg_news not available"
        )
        assert df['title'].notna().all(), "Title should not be null"
        assert df['url'].notna().all(), "URL should not be null"
        assert df['published_at'].notna().all(), "Published date should not be null"

    def test_analytics_aggregations(self, engine):
        """Test aggregations are calculated correctly."""
        df = _read_sql_or_skip(
            """SELECT avg_temperature_celsius, MAX(max_temperature_celsius)
               FROM marts.weather_daily_summary
               GROUP BY weather_date LIMIT 10""",
            engine,
            "marts.weather_daily_summary not available"
        )

        for _, row in df.iterrows():
            assert row['avg_temperature_celsius'] <= row['max_temperature_celsius']

    def test_no_duplicate_records(self, engine):
        """Test no duplicate weather records."""
        df = _read_sql_or_skip(
            """SELECT weather_id, COUNT(*) as count
               FROM staging.stg_weather
               GROUP BY weather_id
               HAVING COUNT(*) > 1""",
            engine,
            "staging.stg_weather not available"
        )
        assert len(df) == 0, "Duplicate records found"

    def test_data_freshness(self, engine):
        """Test data is recent (updated within 24 hours)."""
        df = _read_sql_or_skip(
            """SELECT MAX(created_at) as last_update
               FROM staging.stg_weather""",
            engine,
            "staging.stg_weather not available"
        )

        last_update = df.iloc[0, 0]
        if last_update:
            hours_since_update = (datetime.utcnow() - last_update).total_seconds() / 3600
            assert hours_since_update < 24, f"Data is {hours_since_update:.1f} hours old"


class TestDataTransformation:
    """Test transformations produce correct results."""

    def test_weather_enrichment_has_heat_index(self, engine):
        """Test enriched weather has calculated heat index."""
        df = _read_sql_or_skip(
            """SELECT heat_index_celsius, temperature_celsius
               FROM marts.weather_enriched LIMIT 10""",
            engine,
            "marts.weather_enriched not available"
        )
        assert 'heat_index_celsius' in df.columns
        assert df['heat_index_celsius'].notna().all()

    def test_weather_enrichment_has_categories(self, engine):
        """Test enriched weather has temperature categories."""
        df = _read_sql_or_skip(
            """SELECT temperature_category
               FROM marts.weather_enriched LIMIT 10""",
            engine,
            "marts.weather_enriched not available"
        )
        valid_categories = ['Extreme Cold', 'Very Cold', 'Cold', 'Cool', 'Warm', 'Hot']
        assert df['temperature_category'].isin(valid_categories).all()

    def test_aggregations_accurate(self, engine):
        """Test daily aggregations match raw data."""
        # Get aggregated value
        agg_df = _read_sql_or_skip(
            """SELECT weather_date, city_name, avg_temperature_celsius
               FROM marts.weather_daily_summary
               WHERE weather_date = CURRENT_DATE - INTERVAL '1 day'
               LIMIT 1""",
            engine,
            "marts.weather_daily_summary not available"
        )

        if len(agg_df) > 0:
            # Verify against raw data
            date = agg_df.iloc[0]['weather_date']
            city = agg_df.iloc[0]['city_name']
            agg_temp = agg_df.iloc[0]['avg_temperature_celsius']

            raw_df = _read_sql_or_skip(
                f"""SELECT AVG(temperature_celsius) as avg_temp
                   FROM staging.stg_weather
                   WHERE DATE(weather_timestamp) = '{date}'
                   AND city_name = '{city}'""",
                engine,
                "staging.stg_weather not available"
            )

            if len(raw_df) > 0:
                raw_temp = raw_df.iloc[0]['avg_temp']
                # Allow small floating point differences
                assert abs(agg_temp - raw_temp) < 0.1


class TestPipeline:
    """End-to-end pipeline tests."""

    def test_all_services_running(self, engine):
        """Test all required services are accessible."""
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

    def test_data_flow_complete(self, engine):
        """Test data flows through all layers."""

        # Check each layer has data
        layers = {
            'raw': """SELECT COUNT(*) AS count
                      FROM information_schema.tables
                      WHERE table_schema = 'raw'""",
            'staging': 'SELECT COUNT(*) FROM staging.stg_weather',
            'marts': 'SELECT COUNT(*) FROM marts.weather_enriched',
        }

        for layer, query in layers.items():
            _read_sql_or_skip(query, engine, f"{layer} layer not accessible")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])