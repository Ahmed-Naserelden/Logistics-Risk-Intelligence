#!/bin/bash

# === CONFIGURATION ===
PYTHON="/usr/bin/python3"  # Adjust if different


# === SCRIPT PATHS ===
SCRIPT1="/extraction/scripts/download_ports/script.sh"
SCRIPT2="/extraction/scripts/vessels_scrapper/script.sh"

# === CRON SCHEDULES ===
# Monthly on the 1st at 2:00 AM
CRON1="0 2 1 * * $SCRIPT1 >> $LOGS_DIR/download_ports.log 2>&1"
CRON1="* * * * * $SCRIPT1 >> $LOGS_DIR/download_ports.log 2>&1"

# Daily at 3:00 AM
CRON2="0 3 * * * $SCRIPT2 >> $LOGS_DIR/vessel_scraper.log 2>&1"
CRON2="* * * * * $SCRIPT2 >> $LOGS_DIR/vessel_scraper.log 2>&1"

# === INSTALL CRON JOBS IF NOT EXISTS ===
( crontab -l 2>/dev/null | grep -F "$SCRIPT1" ) >/dev/null || (crontab -l 2>/dev/null; echo "$CRON1") | crontab -
( crontab -l 2>/dev/null | grep -F "$SCRIPT2" ) >/dev/null || (crontab -l 2>/dev/null; echo "$CRON2") | crontab -

echo "Cron jobs set up:"
crontab -l | grep -E "$SCRIPT1|$SCRIPT2"
