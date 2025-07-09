#!/bin/bash

# Start the cron daemon (useful for scheduled tasks)
sudo service cron start

# Start the SSH service
sudo service ssh start


# Create the Debezium PostgreSQL connector only once
# Check if a marker file exists to avoid duplicate connector creation
if [ ! -f "~/conector_created" ]; then

  curl -X POST http://debezium:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "seismic-events-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "postgres",
      "database.password": "postgres",
      "database.dbname": "maritime_logistics",
      "database.server.name": "maritime_logistics_server",
      "plugin.name": "pgoutput",
      "slot.name": "seismic_events_slot",
      "publication.autocreate.mode": "filtered",
      "table.include.list": "public.seismic_events",
      "topic.prefix": "maritime_logistics_server",
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter"
    }
  }' && touch ~/conector_created
  
fi

# Ensure the logs directory exists for the listener logs
if [ ! -d "/extraction/logs" ]; then
  mkdir -p /extraction/logs
fi

# Run the seismic events listener script and log its output (stdout + stderr)
python3 "/extraction/scripts/seismic_events/seismic_listener.py" &>> "/extraction/logs/seismic_listener_wal.log"
