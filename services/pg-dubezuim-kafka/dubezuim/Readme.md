
```bash
# create postgress connector
curl -X POST http://localhost:8093/connectors \
  -H "Content-Type: application/json" \
  -d @seismic_connector.json
```
---
```bash
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
}'

```
---
```bash
# list all topic 
kafka-topics --bootstrap-server localhost:9092 --list
```
---
```bash
# consumer 
kafka-console-consumer --bootstrap-server broker:29092 --topic maritime_logistics_server.public.seismic_events
```
---
```bash
#  testing
UPDATE seismic_events
SET source_catalog = 'EMSC-RTS'
WHERE source_catalog = 'EMSC-RTS';
```
---



