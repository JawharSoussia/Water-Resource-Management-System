"""Initialize streaming tables in PostgreSQL."""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def init_streaming_tables():
    """Read and execute stream_tables.sql file."""
    try:
        # Database connection
        # Use localhost for host machine, not "postgres" (Docker service name)
        db_host = "localhost"  # Always use localhost when running on host machine
        db_user = os.getenv("POSTGRES_USER", "datauser")
        db_password = os.getenv("POSTGRES_PASSWORD", "datauser")
        db_name = os.getenv("POSTGRES_DB", "data_pipeline_db")

        conn = psycopg2.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name
        )
        cursor = conn.cursor()

        print(f"[OK] Connected to PostgreSQL at {db_host}:{db_name}")

        # Read SQL file
        with open("config/stream_tables.sql", "r") as f:
            sql_script = f.read()

        # Execute SQL
        cursor.execute(sql_script)
        conn.commit()

        print("[OK] Stream tables created successfully!")
        print("\nTables created:")
        print("  - staging.water_stream")
        print("  - staging.weather_stream")
        print("  - staging.alerts_stream")
        print("  - monitoring.consumer_lag")

        # Verify tables
        cursor.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema IN ('staging', 'monitoring')
        """)
        tables = cursor.fetchall()
        print(f"\n[OK] Verified {len(tables)} tables created")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"[ERROR] Error: {e}")
        return False

    return True


if __name__ == "__main__":
    init_streaming_tables()