@echo off
REM Windows Consumer Lag Monitoring - Continuous Loop
REM Usage: Run this file to continuously monitor Kafka consumer lag
REM Stop: Press CTRL+C in the command window

setlocal enabledelayedexpansion

color 0A
title Kafka Consumer Lag Monitoring Dashboard

:loop
cls
echo.
echo ============================================================================
echo  KAFKA CONSUMER LAG MONITORING DASHBOARD
echo ============================================================================
echo  Time: %date% %time%
echo  Status: Running (Press CTRL+C to stop)
echo ============================================================================
echo.

REM Run the lag check script
python src/database/check_consumer_lag.py

echo.
echo ============================================================================
echo  Next refresh in 5 seconds... (Press CTRL+C to stop)
echo ============================================================================
echo.

timeout /t 10 /nobreak
goto loop
