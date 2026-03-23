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