"""Verify streaming data in PostgreSQL."""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def verify_streaming_data():
    """Query and display streaming data from PostgreSQL."""
    try:
        # Database connection (use localhost for host machine)
        conn = psycopg2.connect(
            host="localhost",
            user=os.getenv("POSTGRES_USER", "datauser"),
            password=os.getenv("POSTGRES_PASSWORD", "datauser"),
            database=os.getenv("POSTGRES_DB", "data_pipeline_db")
        )
        cursor = conn.cursor()

        print("=" * 70)
        print("[INFO] STREAMING DATA VERIFICATION")
        print("=" * 70)

        # Check weather data
        print("\n[WEATHER] WEATHER STREAM DATA:")
        print("-" * 70)
        cursor.execute("SELECT COUNT(*) FROM staging.weather_stream")
        count = cursor.fetchone()[0]
        print(f"[OK] Total records in weather_stream: {count}")

        if count > 0:
            cursor.execute("""
                SELECT location, temperature, humidity, timestamp
                FROM staging.weather_stream
                ORDER BY ingested_at DESC
                LIMIT 5
            """)
            columns = [desc[0] for desc in cursor.description]
            print(f"\nLatest 5 records:")
            print(f"{columns[0]:20} {columns[1]:15} {columns[2]:15} {columns[3]:25}")
            print("-" * 70)
            for row in cursor.fetchall():
                print(f"{str(row[0]):20} {str(row[1]):15} {str(row[2]):15} {str(row[3]):25}")

        # Check water data
        print("\n\n[WATER] WATER STREAM DATA:")
        print("-" * 70)
        cursor.execute("SELECT COUNT(*) FROM staging.water_stream")
        count = cursor.fetchone()[0]
        print(f"[OK] Total records in water_stream: {count}")

        if count > 0:
            cursor.execute("""
                SELECT dataset_id, temperature, precipitation, timestamp
                FROM staging.water_stream
                ORDER BY ingested_at DESC
                LIMIT 5
            """)
            columns = [desc[0] for desc in cursor.description]
            print(f"\nLatest 5 records:")
            print(f"{columns[0]:20} {columns[1]:15} {columns[2]:15} {columns[3]:25}")
            print("-" * 70)
            for row in cursor.fetchall():
                print(f"{str(row[0]):20} {str(row[1]):15} {str(row[2]):15} {str(row[3]):25}")

        # Check consumer lag
        print("\n\n[LAG] CONSUMER LAG MONITORING:")
        print("-" * 70)
        cursor.execute("SELECT COUNT(*) FROM monitoring.consumer_lag")
        count = cursor.fetchone()[0]
        print(f"[OK] Total lag records: {count}")

        if count > 0:
            cursor.execute("""
                SELECT consumer_group, topic, partition, lag, measured_at
                FROM monitoring.consumer_lag
                ORDER BY measured_at DESC
                LIMIT 5
            """)
            columns = [desc[0] for desc in cursor.description]
            print(f"\nLatest 5 lag measurements:")
            print(f"{columns[0]:25} {columns[1]:25} {columns[2]:12} {columns[3]:10} {columns[4]:25}")
            print("-" * 70)
            for row in cursor.fetchall():
                print(f"{str(row[0]):25} {str(row[1]):25} {str(row[2]):12} {str(row[3]):10} {str(row[4]):25}")

        print("\n" + "=" * 70)
        print("[OK] Verification complete!")
        print("=" * 70)

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"[ERROR] Error: {e}")
        return False


if __name__ == "__main__":
    verify_streaming_data()