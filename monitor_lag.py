"""Continuous consumer lag monitoring script - Windows compatible."""

import os
import time
import subprocess
import sys
from dotenv import load_dotenv

load_dotenv()

def clear_screen():
    """Clear console screen (Windows/Unix compatible)."""
    os.system('cls' if os.name == 'nt' else 'clear')

def run_monitoring(interval=5):
    """
    Continuously monitor consumer lag at specified interval.

    Args:
        interval: Seconds between checks (default: 5)
    """
    print("=" * 100)
    print("[MONITOR] Consumer Lag Monitoring - Press CTRL+C to stop")
    print("=" * 100)
    print(f"[INFO] Checking every {interval} seconds...\n")

    try:
        iteration = 0
        while True:
            iteration += 1
            clear_screen()

            print("=" * 100)
            print(f"[MONITOR] Consumer Lag Monitoring (Check #{iteration})")
            print(f"[TIME] Updated at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 100)
            print()

            # Run the lag check script
            result = subprocess.run(
                [sys.executable, "src/database/check_consumer_lag.py"],
                capture_output=False,
                text=True
            )

            if result.returncode != 0:
                print(f"\n[ERROR] Failed to check lag (exit code: {result.returncode})")

            print("\n[INFO] Next check in {} seconds (Press CTRL+C to stop)...".format(interval))
            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n\n[INFO] Monitoring stopped by user")
        print("[OK] Goodbye!")

if __name__ == "__main__":
    # Get interval from command line argument or use default
    interval = 5
    if len(sys.argv) > 1:
        try:
            interval = int(sys.argv[1])
        except ValueError:
            print(f"[ERROR] Invalid interval: {sys.argv[1]}")
            print("Usage: python monitor_lag.py [interval_seconds]")
            sys.exit(1)

    run_monitoring(interval)
