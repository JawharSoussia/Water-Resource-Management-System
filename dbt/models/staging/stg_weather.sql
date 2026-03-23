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