#!/bin/bash


sudo service cron start

sudo service ssh start

if [ ! -d "/extraction/logs" ]; then
  mkdir -p /extraction/logs
fi

# Run seismic events streaming listener
python3 "/extraction/scripts/seismic_events/seismic_listener.py" &>> "/extraction/logs/seismic_listener_wal.log"