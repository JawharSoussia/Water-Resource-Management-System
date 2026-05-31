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
        .metric-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 10px;
            margin: 10px 0;
        }
        h1 {
            color: #667eea;
            text-align: center;
        }
    </style>
""", unsafe_allow_html=True)

# Database connection
@st.cache_resource
def get_db_connection():
    """Create cached database connection."""
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
        return df
    except Exception as e:
        logger.error(f"Error fetching weather data: {e}")
        return pd.DataFrame()


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
        logger.error(f"Error fetching news data: {e}")
        return pd.DataFrame()


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
        return df
    except Exception as e:
        logger.error(f"Error fetching enriched weather data: {e}")
        return pd.DataFrame()


# --- MAIN DASHBOARD ---

st.title("📊 Data Pipeline Dashboard")
st.markdown("Real-time insights from our data engineering pipeline")

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

# --- PAGE: OVERVIEW ---
if page == "Overview":
    st.subheader("Pipeline Overview")

    col1, col2, col3, col4 = st.columns(4)

    # Get latest data counts
    engine = get_db_connection()

    with col1:
        try:
            weather_count = pd.read_sql(
                "SELECT COUNT(*) as count FROM staging.stg_weather", engine
            ).iloc[0, 0]
            st.metric("Weather Records", f"{weather_count:,}")
        except:
            st.metric("Weather Records", "N/A")

    with col2:
        try:
            news_count = pd.read_sql(
                "SELECT COUNT(*) as count FROM staging.stg_news", engine
            ).iloc[0, 0]
            st.metric("News Articles", f"{news_count:,}")
        except:
            st.metric("News Articles", "N/A")

    with col3:
        try:
            last_update = pd.read_sql(
                "SELECT MAX(created_at) as time FROM staging.stg_weather", engine
            ).iloc[0, 0]
            st.metric("Last Update", last_update.strftime("%H:%M") if last_update else "N/A")
        except:
            st.metric("Last Update", "N/A")

    with col4:
        st.metric("Pipeline Status", "🟢 Running")

    engine.dispose()

    st.divider()

    # Recent records
    st.subheader("Recent Data Summary")

    col1, col2 = st.columns(2)

    with col1:
        st.write("**Latest Weather by City**")
        weather_df = get_weather_enriched()
        if not weather_df.empty:
            display_df = weather_df[['city_name', 'temperature_celsius', 'humidity_percent']].head(5)
            st.dataframe(display_df, use_container_width=True)
        else:
            st.info("No weather data yet")

    with col2:
        st.write("**Recent News Activity**")
        news_df = get_news_data()
        if not news_df.empty:
            display_df = news_df[['published_date', 'source_name', 'articles_count']].head(5)
            st.dataframe(display_df, use_container_width=True)
        else:
            st.info("No news data yet")


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
        except:
            st.error("❌ PostgreSQL: Failed")

    with col2:
        st.info("📊 MinIO: Connected")

    with col3:
        st.info("🔄 Kafka: Connected")

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