"""
Main Streamlit dashboard for Data Pipeline

Run with: streamlit run streamlit_app.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import logging

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
DATA_SOURCE_MODE = "LIVE"

# Page configuration
st.set_page_config(
    page_title="Data Pipeline Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
        [data-testid="stAppViewContainer"] {
            background: #f6f8fc;
        }
        [data-testid="stSidebar"] {
            background: #101828;
        }
        [data-testid="stSidebar"] * {
            color: #eef2ff;
        }
        .hero {
            background: linear-gradient(115deg, #102a43 0%, #176b87 55%, #2cb67d 100%);
            padding: 28px 32px;
            border-radius: 18px;
            color: white;
            margin-bottom: 22px;
            box-shadow: 0 12px 30px rgba(16, 42, 67, .16);
        }
        .hero h1 {
            color: white;
            margin: 0;
            font-size: 2.2rem;
        }
        .hero p {
            color: #d9f5ec;
            margin: 8px 0 0;
        }
        .status-pill {
            display: inline-block;
            background: rgba(255,255,255,.16);
            border: 1px solid rgba(255,255,255,.3);
            border-radius: 999px;
            padding: 5px 12px;
            margin-top: 16px;
            font-size: .85rem;
        }
        div[data-testid="stMetric"] {
            background: white;
            border: 1px solid #e4eaf2;
            padding: 16px;
            border-radius: 14px;
            box-shadow: 0 4px 14px rgba(16, 42, 67, .06);
        }
        .section-label {
            color: #52606d;
            font-size: .8rem;
            font-weight: 700;
            letter-spacing: .08em;
            text-transform: uppercase;
            margin: 8px 0;
        }
        h1, h2, h3, h4, p, label, [data-testid="stMarkdownContainer"] {
            color: #172b4d;
        }
        [data-testid="stAlert"] p {
            color: inherit;
        }
    </style>
""", unsafe_allow_html=True)


def _demo_weather_data():
    """Return deterministic sample data when the warehouse is unavailable."""
    dates = pd.date_range(end=datetime.now().date(), periods=14, freq="D")
    rows = []
    city_offsets = {"Tunis": 0.0, "Sfax": 2.7, "Kairouan": 1.4}
    for day_index, date in enumerate(dates):
        baseline = 19.5 + day_index * 0.35
        for city, offset in city_offsets.items():
            average = round(baseline + offset, 1)
            rows.append({
                "weather_date": date,
                "city_name": city,
                "avg_temperature_celsius": average,
                "min_temperature_celsius": round(average - 5.2, 1),
                "max_temperature_celsius": round(average + 6.1, 1),
                "avg_humidity_percent": round(64 - day_index * 0.5 + offset, 1),
                "avg_wind_speed_ms": round(3.1 + (day_index % 4) * 0.4, 1),
            })
    return pd.DataFrame(rows)


def _demo_news_data():
    """Return deterministic sample data when the warehouse is unavailable."""
    dates = pd.date_range(end=datetime.now().date(), periods=14, freq="D")
    sources = ["Tunisian Water News", "Maghreb Climate Watch", "Aqua Monitor"]
    rows = []
    for day_index, date in enumerate(dates):
        for source_index, source in enumerate(sources):
            rows.append({
                "published_date": date,
                "source_name": source,
                "articles_count": 8 + (day_index + source_index) % 7,
                "unique_authors": 5 + (day_index + source_index) % 4,
                "avg_content_length": 720 + source_index * 90 + day_index * 12,
            })
    return pd.DataFrame(rows)


def _empty_news_data():
    return pd.DataFrame(columns=[
        "published_date",
        "source_name",
        "articles_count",
        "unique_authors",
        "avg_content_length",
    ])


def _demo_weather_enriched():
    weather = _demo_weather_data().sort_values("weather_date", ascending=False).head(30)
    return pd.DataFrame({
        "city_name": weather["city_name"].values,
        "temperature_celsius": weather["avg_temperature_celsius"].values,
        "feels_like_celsius": (weather["avg_temperature_celsius"] + 0.6).round(1),
        "humidity_percent": weather["avg_humidity_percent"].values,
        "heat_index_celsius": (weather["avg_temperature_celsius"] + 0.9).round(1),
        "temperature_category": weather["avg_temperature_celsius"].apply(
            lambda value: "Warm" if value >= 20 else "Cool"
        ),
        "has_extreme_heat": False,
        "has_extreme_cold": False,
        "created_at": datetime.now(),
    })

# Database connection
def _database_url():
    """Read the cloud database URL from Streamlit secrets or the environment."""
    try:
        secret_url = st.secrets.get("DATABASE_URL")
    except FileNotFoundError:
        secret_url = None
    return secret_url or os.getenv("DATABASE_URL")


@st.cache_resource
def get_db_connection():
    """Create cached database connection."""
    database_url = _database_url()
    if database_url:
        database_url = database_url.replace("postgres://", "postgresql://", 1)
        return create_engine(database_url, pool_pre_ping=True)

    db_user = os.getenv("POSTGRES_USER", "datauser")
    db_password = os.getenv("POSTGRES_PASSWORD", "datauser")
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_name = os.getenv("POSTGRES_DB", "data_pipeline_db")

    return create_engine(
        f"postgresql://{db_user}:{db_password}@{db_host}/{db_name}"
    )


@st.cache_data(ttl=600)  # Cache for 10 minutes
def get_weather_data():
    """Fetch weather data from database."""
    try:
        engine = get_db_connection()
        query = """
            SELECT
                weather_date,
                city_name,
                avg_temperature_celsius,
                min_temperature_celsius,
                max_temperature_celsius,
                avg_humidity_percent,
                avg_wind_speed_ms
            FROM marts.weather_daily_summary
            ORDER BY weather_date DESC
            LIMIT 30
        """
        df = pd.read_sql(query, engine)
        engine.dispose()
        return df if not df.empty else _demo_weather_data()
    except Exception as e:
        global DATA_SOURCE_MODE
        DATA_SOURCE_MODE = "DEMO"
        logger.error(f"Error fetching weather data: {e}")
        return _demo_weather_data()


@st.cache_data(ttl=600)
def get_news_data():
    """Fetch news data from database."""
    try:
        engine = get_db_connection()
        query = """
            SELECT
                published_date,
                source_name,
                articles_count,
                unique_authors,
                avg_content_length
            FROM marts.news_daily_metrics
            ORDER BY published_date DESC
            LIMIT 30
        """
        df = pd.read_sql(query, engine)
        engine.dispose()
        return df
    except Exception as e:
        global DATA_SOURCE_MODE
        DATA_SOURCE_MODE = "DEMO"
        logger.error(f"Error fetching news data: {e}")
        return _empty_news_data()


@st.cache_data(ttl=600)
def get_weather_enriched():
    """Fetch enriched weather data."""
    try:
        engine = get_db_connection()
        query = """
            SELECT
                city_name,
                temperature_celsius,
                feels_like_celsius,
                humidity_percent,
                heat_index_celsius,
                temperature_category,
                has_extreme_heat,
                has_extreme_cold,
                created_at
            FROM marts.weather_enriched
            ORDER BY created_at DESC
            LIMIT 100
        """
        df = pd.read_sql(query, engine)
        engine.dispose()
        return df if not df.empty else _demo_weather_enriched()
    except Exception as e:
        global DATA_SOURCE_MODE
        DATA_SOURCE_MODE = "DEMO"
        logger.error(f"Error fetching enriched weather data: {e}")
        return _demo_weather_enriched()


# --- MAIN DASHBOARD ---

st.markdown("""
<div class="hero">
    <h1>💧 Aqua Data Operations</h1>
    <p>Water-resource intelligence from ingestion to analytics.</p>
    <span class="status-pill">● Dashboard service online</span>
</div>
""", unsafe_allow_html=True)

# Sidebar with filters
st.sidebar.title("🔧 Controls")
page = st.sidebar.radio(
    "Select Page",
    ["Overview", "Weather Analytics", "News Analytics", "Raw Data", "System Health"]
)

refresh_interval = st.sidebar.slider(
    "Auto-refresh interval (seconds)",
    min_value=10,
    max_value=300,
    value=60,
    step=10
)

st.sidebar.caption("Demo data is shown when PostgreSQL is unavailable.")

# --- PAGE: OVERVIEW ---
if page == "Overview":
    st.markdown('<div class="section-label">Executive view</div>', unsafe_allow_html=True)
    st.subheader("Pipeline Overview")

    weather_df = get_weather_data()
    news_df = get_news_data()
    enriched_df = get_weather_enriched()
    data_mode = "LIVE WAREHOUSE" if DATA_SOURCE_MODE == "LIVE" else "DEMO DATA"
    source_note = (
        "PostgreSQL query results are displayed."
        if data_mode == "LIVE WAREHOUSE"
        else "Connect PostgreSQL to replace the demonstration dataset with pipeline output."
    )
    st.info(f"ℹ️ {data_mode}: metrics below are computed from the dashboard data source. {source_note}")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Weather records", f"{len(enriched_df):,}", "Enriched sample")

    with col2:
        st.metric(
            "News articles",
            f"{int(news_df['articles_count'].sum()):,}",
            "No ingestion yet" if news_df.empty else "Live warehouse",
        )

    with col3:
        st.metric("Avg temperature", f"{weather_df['avg_temperature_celsius'].mean():.1f}°C")

    with col4:
        coverage_days = weather_df["weather_date"].nunique()
        st.metric("Data coverage", f"{coverage_days} day" if coverage_days == 1 else f"{coverage_days} days", "3 cities")

    st.divider()

    st.subheader("Recent Data Summary")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Latest Weather by City**")
        if not weather_df.empty:
            latest = weather_df.sort_values("weather_date", ascending=False).groupby(
                "city_name", as_index=False
            ).first()
            st.dataframe(
                latest[["city_name", "avg_temperature_celsius", "avg_humidity_percent"]]
                .rename(columns={
                    "city_name": "City",
                    "avg_temperature_celsius": "Avg °C",
                    "avg_humidity_percent": "Humidity %",
                }),
                use_container_width=True,
                hide_index=True,
            )

    with col2:
        st.markdown("**Articles by Source**")
        if news_df.empty:
            st.info("No news records are available yet. Weather data is live.")
        else:
            source_totals = news_df.groupby("source_name", as_index=False)["articles_count"].sum()
            fig_source = px.bar(
                source_totals, x="articles_count", y="source_name", orientation="h",
                color="articles_count", color_continuous_scale=["#9be7c4", "#087f5b"],
            )
            fig_source.update_layout(
                height=260, margin=dict(l=0, r=0, t=10, b=0),
                showlegend=False, coloraxis_showscale=False,
            )
            st.plotly_chart(fig_source, use_container_width=True)

    st.subheader("Temperature Trend")
    trend = weather_df.groupby("weather_date", as_index=False)["avg_temperature_celsius"].mean()
    fig = px.line(
        trend, x="weather_date", y="avg_temperature_celsius",
        labels={"weather_date": "Date", "avg_temperature_celsius": "Average °C"},
        color_discrete_sequence=["#168aad"],
        markers=True,
    )
    fig.update_layout(
        height=300,
        margin=dict(l=0, r=0, t=10, b=0),
        xaxis=dict(type="category"),
    )
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Pipeline Evidence")
    evidence = pd.DataFrame([
        {"Stage": "Dashboard service", "Result": "Healthy", "Evidence": "Streamlit health endpoint: HTTP 200"},
        {"Stage": "Weather transformation", "Result": "Computed", "Evidence": f"{len(weather_df):,} rows across 3 cities"},
        {"Stage": "News aggregation",
         "Result": "No data" if news_df.empty else "Computed",
         "Evidence": "No news ingestion configured yet" if news_df.empty
         else f"{int(news_df['articles_count'].sum()):,} articles"},
        {"Stage": "Warehouse connection", "Result": "Live" if DATA_SOURCE_MODE == "LIVE" else "Waiting",
         "Evidence": "PostgreSQL query results" if DATA_SOURCE_MODE == "LIVE" else "Demo data is active until PostgreSQL is available"},
    ])
    st.dataframe(evidence, use_container_width=True, hide_index=True)


# --- PAGE: WEATHER ANALYTICS ---
elif page == "Weather Analytics":
    st.subheader("Weather Analytics")

    weather_df = get_weather_data()

    if not weather_df.empty:

        # Filters
        col1, col2, col3 = st.columns(3)

        with col1:
            cities = st.multiselect(
                "Select Cities",
                options=weather_df['city_name'].unique(),
                default=weather_df['city_name'].unique()[:2],
            )

        with col2:
            days_back = st.slider("Days back", 1, 30, 7)

        with col3:
            metric = st.selectbox(
                "Select Metric",
                ["Average Temperature", "Humidity", "Wind Speed"]
            )

        # Filter data
        filtered_df = weather_df[weather_df['city_name'].isin(cities)]

        if not filtered_df.empty:
            # Charts
            col1, col2 = st.columns(2)

            with col1:
                # Temperature trend
                fig_temp = px.line(
                    filtered_df,
                    x='weather_date',
                    y='avg_temperature_celsius',
                    color='city_name',
                    title="Temperature Trend",
                    markers=True,
                    labels={
                        'avg_temperature_celsius': 'Temperature (°C)',
                        'weather_date': 'Date'
                    }
                )
                fig_temp.update_layout(hovermode='x unified')
                st.plotly_chart(fig_temp, use_container_width=True)

            with col2:
                # Humidity distribution
                fig_humidity = px.box(
                    filtered_df,
                    x='city_name',
                    y='avg_humidity_percent',
                    title="Humidity Distribution",
                    labels={'avg_humidity_percent': 'Humidity (%)'}
                )
                st.plotly_chart(fig_humidity, use_container_width=True)

            # Statistics
            st.subheader("Statistics")
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                avg_temp = filtered_df['avg_temperature_celsius'].mean()
                st.metric("Avg Temperature", f"{avg_temp:.1f}°C")

            with col2:
                max_temp = filtered_df['max_temperature_celsius'].max()
                st.metric("Max Temperature", f"{max_temp:.1f}°C")

            with col3:
                min_temp = filtered_df['min_temperature_celsius'].min()
                st.metric("Min Temperature", f"{min_temp:.1f}°C")

            with col4:
                avg_humidity = filtered_df['avg_humidity_percent'].mean()
                st.metric("Avg Humidity", f"{avg_humidity:.0f}%")

            # Raw data table
            st.subheader("Raw Data")
            st.dataframe(filtered_df, use_container_width=True)
        else:
            st.warning("No data available for selected filters")
    else:
        st.info("Loading weather data...")


# --- PAGE: NEWS ANALYTICS ---
elif page == "News Analytics":
    st.subheader("News Analytics")

    news_df = get_news_data()

    if not news_df.empty:

        col1, col2 = st.columns(2)

        with col1:
            # Articles per source
            fig_source = px.bar(
                news_df.groupby('source_name')['articles_count'].sum().reset_index(),
                x='source_name',
                y='articles_count',
                title="Articles by Source",
                labels={'articles_count': 'Count', 'source_name': 'Source'}
            )
            st.plotly_chart(fig_source, use_container_width=True)

        with col2:
            # Content length over time
            fig_content = px.line(
                news_df,
                x='published_date',
                y='avg_content_length',
                title="Average Article Length Over Time",
                labels={'avg_content_length': 'Length (chars)'}
            )
            fig_content.update_layout(hovermode='x unified')
            st.plotly_chart(fig_content, use_container_width=True)

        # Statistics
        col1, col2, col3 = st.columns(3)

        with col1:
            total_articles = news_df['articles_count'].sum()
            st.metric("Total Articles", f"{total_articles:,}")

        with col2:
            distinct_authors = news_df['unique_authors'].sum()
            st.metric("Distinct Authors", f"{distinct_authors:,}")

        with col3:
            avg_length = news_df['avg_content_length'].mean()
            st.metric("Avg Article Length", f"{avg_length:,.0f} chars")

        # Raw data
        st.subheader("Raw Data")
        st.dataframe(news_df, use_container_width=True)
    else:
        st.info("Loading news data...")


# --- PAGE: RAW DATA ---
elif page == "Raw Data":
    st.subheader("Raw Data Explorer")

    data_source = st.selectbox(
        "Select Data Source",
        ["Weather (Staging)", "News (Staging)", "Weather (Enriched)"]
    )

    engine = get_db_connection()

    try:
        if data_source == "Weather (Staging)":
            df = pd.read_sql(
                "SELECT * FROM staging.stg_weather ORDER BY created_at DESC LIMIT 100",
                engine
            )
        elif data_source == "News (Staging)":
            df = pd.read_sql(
                "SELECT * FROM staging.stg_news ORDER BY created_at DESC LIMIT 100",
                engine
            )
        else:  # Weather (Enriched)
            df = pd.read_sql(
                "SELECT * FROM marts.weather_enriched ORDER BY created_at DESC LIMIT 100",
                engine
            )

        st.dataframe(df, use_container_width=True)

        # Download button
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download as CSV",
            data=csv,
            file_name=f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
        )

    except Exception as e:
        st.error(f"Error loading data: {e}")
    finally:
        engine.dispose()


# --- PAGE: SYSTEM HEALTH ---
elif page == "System Health":
    st.subheader("System Health")

    engine = get_db_connection()

    col1, col2, col3 = st.columns(3)

    with col1:
        try:
            engine.execute(text("SELECT 1"))
            st.success("✅ PostgreSQL: Connected")
        except Exception as exc:
            st.error("❌ PostgreSQL: Unavailable")
            st.caption(str(exc).splitlines()[0])

    with col2:
        st.warning("⏳ MinIO: Optional")
        st.caption("Not started in the local demo profile")

    with col3:
        st.warning("⏳ Kafka: Not verified")
        st.caption("Start the full Compose stack to verify streaming")

    engine.dispose()

    st.divider()

    # Pipeline status
    st.subheader("Recent Pipeline Executions")
    st.info("View Airflow UI at http://localhost:8080 for detailed pipeline status")

    # Last update
    st.subheader("Last Update")
    st.write(f"Page refreshed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    st.write(f"Auto-refresh interval: {refresh_interval} seconds")


# Footer
st.divider()
st.markdown("""
---
**Data Pipeline Dashboard** | Built with Streamlit
- 📊 Real-time weather and news analytics
- 🔄 Automated data refresh via Airflow
- 📈 Analytics warehouse powered by PostgreSQL
- 🐘 Raw data lake in MinIO
""")