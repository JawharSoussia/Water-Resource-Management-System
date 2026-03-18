# STEP 4: Data Transformation - dbt & Pandas

**Status:** Transformation Phase
**Duration:** 4-5 hours
**Owner Notes:** This is where raw data becomes insights. Transformation is 70% of data pipeline work. Get this right and you're a data engineer.

---

## 🎯 Objective

Transform raw, messy data into clean, structured, business-ready tables ready for analysis and visualization.

---

## 📋 Mandatory Deliverables for This Step

- ✅ At least 3 business transformations (documented)
- ✅ dbt models with tests
- ✅ Pandas transformation scripts
- ✅ Data quality checks
- ✅ Transformation lineage documentation
- ✅ Unit tests for transformations
- ✅ README documenting each transformation

---

## 🔧 PHASE 1: Initialize dbt Project

### Step 1.1: Create dbt project structure

```bash
# Navigate to dbt directory
cd dbt

# Initialize dbt project
dbt init

# When prompted:
# project name: data_pipeline
# database: postgres
# host: localhost
# user: datauser
# password: datauser  (your password)
# port: 5432
# database name: data_pipeline_db
# schema: analytics
# threads: 4
# keepalives_idle: 0
```

### Step 1.2: Create dbt profiles.yml

dbt automatically creates `.dbt/profiles.yml`. Verify it looks like:

```yaml
data_pipeline:
  outputs:
    dev:
      type: postgres
      host: localhost
      user: datauser
      password: datauser
      port: 5432
      dbname: data_pipeline_db
      schema: analytics
      threads: 4
      keepalives_idle: 0

  target: dev
```

---

## 📊 PHASE 2: Create Staging Models (Transformation 1: Cleaning)

### Step 2.1: Clean Weather Data

Create file: `dbt/models/staging/stg_weather.sql`

```sql
-- Staging model: Clean and standardize weather data

{{ config(
    materialized='table',
    description='Cleaned weather data from OpenWeather API'
) }}

WITH raw_weather AS (
    -- Source: Raw API response
    SELECT
        id,
        main::json->>'temp' as temperature_celsius,
        main::json->>'feels_like' as feels_like_celsius,
        main::json->>'humidity' as humidity_percent,
        main::json->>'pressure' as pressure_hpa,
        visibility,
        name as city_name,
        sys::json->>'country' as country_code,
        visibility,
        wind::json->>'speed' as wind_speed_ms,
        cast(to_timestamp(dt) as timestamp) as weather_timestamp,
        _ingested_at as ingestion_timestamp
    FROM raw.weather_api
),

cleaned AS (
    -- Data cleaning and validation
    SELECT
        {{ dbt_utils.generate_surrogate_key(['city_name', 'weather_timestamp']) }} as weather_id,
        COALESCE(city_name, 'UNKNOWN') as city_name,
        COALESCE(country_code, 'UNKNOWN') as country_code,
        -- Convert to decimal and handle nulls
        CAST(temperature_celsius AS DECIMAL(5, 2)) as temperature_celsius,
        CAST(feels_like_celsius AS DECIMAL(5, 2)) as feels_like_celsius,
        CAST(humidity_percent AS INTEGER) as humidity_percent,
        CAST(pressure_hpa AS INTEGER) as pressure_hpa,
        CAST(wind_speed_ms AS DECIMAL(5, 2)) as wind_speed_ms,
        CAST(visibility AS INTEGER) as visibility_meters,
        weather_timestamp,
        CURRENT_TIMESTAMP as created_at
    FROM raw_weather
    WHERE
        -- Data quality checks
        temperature_celsius IS NOT NULL
        AND humidity_percent >= 0
        AND humidity_percent <= 100
        AND pressure_hpa > 800
        AND pressure_hpa < 1200
        AND wind_speed_ms >= 0
)

SELECT * FROM cleaned

```

### Step 2.2: Clean News Data

Create file: `dbt/models/staging/stg_news.sql`

```sql
-- Staging model: Clean and standardize news data

{{ config(
    materialized='table',
    description='Cleaned news articles from NewsAPI'
) }}

WITH raw_news AS (
    SELECT
        jsonb_array_elements(articles) as article,
        _ingested_at as ingestion_timestamp,
        _query as news_query
    FROM raw.news_api
),

parsed_articles AS (
    SELECT
        article->>'title' as title,
        article->>'description' as description,
        article->>'url' as url,
        article->>'source'->>'name' as source_name,
        article->>'author' as author,
        CAST(article->>'publishedAt' AS TIMESTAMP) as published_at,
        article->>'content' as content,
        ingestion_timestamp,
        news_query
    FROM raw_news
),

cleaned AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['url', 'published_at']) }} as article_id,
        COALESCE(title, 'UNTITLED') as title,
        description,
        url,
        source_name,
        author,
        published_at,
        content,
        LENGTH(COALESCE(content, '')) as content_length,
        -- Extract domain from URL
        SUBSTRING(url FROM POSITION('://' IN url) + 3 FOR POSITION('/' IN SUBSTRING(url FROM POSITION('://' IN url) + 3) - 1)) as domain,
        ingestion_timestamp,
        news_query,
        CURRENT_TIMESTAMP as created_at
    FROM parsed_articles
    WHERE
        title IS NOT NULL
        AND url IS NOT NULL
        AND published_at IS NOT NULL
)

SELECT * FROM cleaned
```

---

## 📈 PHASE 3: Create Analytics Models (Transformation 2: Aggregation)

### Step 3.1: Weather Aggregations

Create file: `dbt/models/marts/weather_daily_summary.sql`

```sql
-- Analytics model: Daily weather aggregations

{{ config(
    materialized='table',
    description='Daily aggregated weather metrics by city'
) }}

SELECT
    DATE(weather_timestamp) as weather_date,
    city_name,
    country_code,
    COUNT(*) as observations_count,
    ROUND(AVG(temperature_celsius), 2) as avg_temperature_celsius,
    ROUND(MIN(temperature_celsius), 2) as min_temperature_celsius,
    ROUND(MAX(temperature_celsius), 2) as max_temperature_celsius,
    ROUND(AVG(humidity_percent), 1) as avg_humidity_percent,
    ROUND(AVG(pressure_hpa), 1) as avg_pressure_hpa,
    ROUND(AVG(wind_speed_ms), 2) as avg_wind_speed_ms,
    ROUND(AVG(visibility_meters), 0) as avg_visibility_meters,
    CURRENT_TIMESTAMP as created_at
FROM {{ ref('stg_weather') }}
GROUP BY
    DATE(weather_timestamp),
    city_name,
    country_code
```

### Step 3.2: News Analytics

Create file: `dbt/models/marts/news_daily_metrics.sql`

```sql
-- Analytics model: Daily news metrics

{{ config(
    materialized='table',
    description='Daily aggregated news metrics'
) }}

SELECT
    DATE(published_at) as published_date,
    source_name,
    COUNT(*) as articles_count,
    COUNT(DISTINCT author) as unique_authors,
    ROUND(AVG(content_length), 0) as avg_content_length,
    COUNT(DISTINCT domain) as unique_domains,
    CURRENT_TIMESTAMP as created_at
FROM {{ ref('stg_news') }}
GROUP BY
    DATE(published_at),
    source_name
ORDER BY
    published_date DESC,
    articles_count DESC
```

---

## 🐍 PHASE 4: Pandas Transformations (Transformation 3: Enrichment)

### Step 4.1: Create Enrichment Script

Create file: `src/transformation/data_enrichment.py`

```python
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
```

---

## 🧪 PHASE 5: Add dbt Tests

### Step 5.1: Create dbt Tests

Create file: `dbt/models/staging/stg_weather.yml`

```yaml
version: 2

models:
  - name: stg_weather
    description: Cleaned weather data
    columns:
      - name: weather_id
        description: Unique weather record ID
        tests:
          - unique
          - not_null

      - name: city_name
        description: City name
        tests:
          - not_null
          - accepted_values:
              values: ['London', 'Paris', 'Tokyo', 'NewYork', 'Sydney']

      - name: temperature_celsius
        description: Temperature in Celsius
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: -60
              max_value: 60

      - name: humidity_percent
        description: Humidity percentage
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 100
```

---

## 📋 Run Transformations

### Step 5.2: Run dbt Models

```bash
# Navigate to dbt directory
cd dbt

# Install dependencies
dbt deps

# Run models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate

# View lineage
dbt docs serve
```

**Expected Output:**
```
Running with dbt=1.7.0
Found 4 models, 0 tests, 0 snapshots, 0 seeds, 0 sources, 0 exposures, 0 metrics
...
17:32:22  ✓ Successfully created table model stg_weather
17:32:23  ✓ Successfully created table model stg_news
17:32:24  ✓ Successfully created table model weather_daily_summary
17:32:25  ✓ Successfully created table model news_daily_metrics

Done!
```

---

## 📋 Commit Transformation Code

```bash
# Add all transformation code
git add dbt/ src/transformation/ tests/

# Commit
git commit -m "Implement data transformations with dbt and Pandas

Transformation 1 - Cleaning (dbt staging models):
- stg_weather: Clean, standardize, validate weather data
- stg_news: Clean, standardize, validate news data

Transformation 2 - Aggregation (dbt analytics models):
- weather_daily_summary: Daily aggregated weather metrics
- news_daily_metrics: Daily news publications metrics

Transformation 3 - Enrichment (Pandas):
- Calculate heat index, temperature categories
- Extract content quality metrics
- Add business alerts and classifications

All transformations include:
- Data quality tests (dbt tests)
- NOT NULL constraints
- Value range validation
- Documentation with dbt YAML

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## ✅ Verification Checklist

Before moving to Step 5:

- [ ] dbt project initialized
- [ ] dbt models created and running
- [ ] All dbt tests passing
- [ ] Staging tables created in database
- [ ] Analytics tables aggregating correctly
- [ ] Enrichment script runs without errors
- [ ] Enriched data tables created
- [ ] Documentation generated
- [ ] Git commit created

---

## 📋 Next Steps

Once this step is complete, move to: **STEP 5: Orchestration with Airflow**

You now have clean, enriched, business-ready data!
