"""Check consumer lag data stored in PostgreSQL monitoring table."""

import os
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

def check_consumer_lag():
    """Query and display consumer lag metrics from monitoring table."""
    try:
        # Database connection (use localhost for host machine)
        conn = psycopg2.connect(
            host="localhost",
            user=os.getenv("POSTGRES_USER", "datauser"),
            password=os.getenv("POSTGRES_PASSWORD", "datauser"),
            database=os.getenv("POSTGRES_DB", "data_pipeline_db")
        )
        cursor = conn.cursor()

        print("=" * 90)
        print("[LAG] CONSUMER LAG MONITORING DATA")
        print("=" * 90)

        # Check if table has data
        cursor.execute("SELECT COUNT(*) FROM monitoring.consumer_lag")
        total_records = cursor.fetchone()[0]
        print(f"\n[OK] Total lag records in database: {total_records}\n")

        if total_records == 0:
            print("[WARN] No lag data found. Run the monitor first:")
            print("   python src/monitoring/kafka_monitor.py\n")
            return True

        # Get latest lag data for each consumer group and topic
        print("[METRICS] LATEST LAG METRICS (by topic and consumer group):")
        print("-" * 90)
        
        cursor.execute("""
            SELECT 
                consumer_group,
                topic,
                partition,
                current_offset,
                log_end_offset,
                lag,
                measured_at
            FROM monitoring.consumer_lag
            ORDER BY measured_at DESC
            LIMIT 20
        """)
        
        results = cursor.fetchall()
        
        # Group by topic and consumer_group
        grouped = {}
        for row in results:
            consumer_group, topic, partition, current_offset, log_end_offset, lag, measured_at = row
            key = (consumer_group, topic)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append({
                'partition': partition,
                'current_offset': current_offset,
                'log_end_offset': log_end_offset,
                'lag': lag,
                'measured_at': measured_at
            })
        
        # Display grouped results
        for (consumer_group, topic), metrics in grouped.items():
            print(f"\n[GROUP] Consumer Group: {consumer_group}")
            print(f"   Topic: {topic}")
            print(f"   {'-' * 86}")
            print(f"   {'Partition':<12} {'Cur Offset':<15} {'End Offset':<15} {'Lag':<12} {'Measured At':<35}")
            print(f"   {'-' * 86}")

            total_lag = 0
            for metric in metrics:
                partition = metric['partition']
                cur_offset = metric['current_offset']
                end_offset = metric['log_end_offset']
                lag = metric['lag']
                measured_at = metric['measured_at']
                total_lag += lag

                lag_status = "[OK]" if lag <= 1 else "[WARN]"
                print(f"   {partition:<12} {cur_offset:<15} {end_offset:<15} {lag:<12} {str(measured_at):<35} {lag_status}")

            print(f"   Total lag for this group: {total_lag}")

        # Show lag trend over time
        print("\n\n[TREND] LAG TREND (Last 10 measurements):")
        print("-" * 90)
        cursor.execute("""
            SELECT 
                consumer_group,
                topic,
                SUM(lag) as total_lag,
                measured_at
            FROM monitoring.consumer_lag
            GROUP BY consumer_group, topic, measured_at
            ORDER BY measured_at DESC
            LIMIT 10
        """)
        
        trend_results = cursor.fetchall()
        print(f"{'Consumer Group':<25} {'Topic':<30} {'Total Lag':<15} {'Measured At':<35}")
        print("-" * 90)
        for row in trend_results:
            consumer_group, topic, total_lag, measured_at = row
            status = "[OK]" if total_lag <= 0 else "[WARN]"
            print(f"{str(consumer_group):<25} {str(topic):<30} {str(total_lag):<15} {str(measured_at):<35} {status}")

        # Summary statistics
        print("\n\n[SUMMARY] SUMMARY STATISTICS:")
        print("-" * 90)
        
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT consumer_group) as num_consumer_groups,
                COUNT(DISTINCT topic) as num_topics,
                AVG(lag) as avg_lag,
                MAX(lag) as max_lag,
                MIN(lag) as min_lag
            FROM monitoring.consumer_lag
            WHERE measured_at > NOW() - INTERVAL '1 hour'
        """)
        
        stats = cursor.fetchone()
        if stats:
            num_groups, num_topics, avg_lag, max_lag, min_lag = stats
            print(f"Consumer Groups:    {num_groups}")
            print(f"Topics:             {num_topics}")
            print(f"Average Lag:        {avg_lag:.2f}" if avg_lag else "Average Lag:        N/A")
            print(f"Max Lag:            {max_lag}")
            print(f"Min Lag:            {min_lag}")

        print("\n" + "=" * 90)
        print("[OK] Lag check complete!")
        print("=" * 90 + "\n")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"[ERROR] Error: {e}")
        return False


if __name__ == "__main__":
    check_consumer_lag()