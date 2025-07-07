#!/bin/bash


service cron start

# Run seismic events streaming listener
/usr/local/bin/python3 /extraction/scripts/seismic_events/seismic_listener.py >> /extraction/logs/seismic_listener_wal.log

tail -f /dev/null
