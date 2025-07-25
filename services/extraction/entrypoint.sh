#!/bin/bash


sudo service cron start

sudo service ssh start

# Ensure the logs directory exists for the listener logs
if [ ! -d "/extraction/logs" ]; then
  mkdir -p /extraction/logs
fi

# Copy data from extraction to /bucket:name volume[shared_storage]
cp -r /extraction/data/* /bucket/


# Run seismic events streaming listener
python3 "/extraction/scripts/seismic_events/seismic_listener.py" &>> "/extraction/logs/seismic_listener_wal.log"
