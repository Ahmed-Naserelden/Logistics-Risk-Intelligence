# Debezium + PostgreSQL + Kafka: Streaming `seismic_events` with CDC

This guide walks you through setting up a Debezium connector to stream changes from the `seismic_events` table in PostgreSQL into Kafka using Change Data Capture (CDC).

---

## Create the Connector (using a JSON config file)

If you already have the connector config saved as `seismic_connector.json`, create the connector like this:

```bash
curl -X POST http://localhost:8093/connectors \
  -H "Content-Type: application/json" \
  -d @seismic_connector.json
```

---

## Or Create the Connector Inline (directly in terminal)

Alternatively, you can define and create the connector inline:

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

## Check Kafka Topics

Make sure your Kafka topic for `seismic_events` was created:

```bash
kafka-topics --bootstrap-server localhost:9092 --list
```

You should see a topic like:

```
maritime_logistics_server.public.seismic_events
```

---

## Consume Change Events

Use this command to read CDC messages from the topic:

```bash
kafka-console-consumer \
  --bootstrap-server broker:29092 \
  --topic maritime_logistics_server.public.seismic_events \
  --from-beginning
```

---

## Test the Stream

Trigger a CDC event by updating the `seismic_events` table in PostgreSQL:

```sql
UPDATE seismic_events
SET source_catalog = 'EMSC-RTS'
WHERE source_catalog = 'EMSC-RTS';
```

You should immediately see a change event show up in your Kafka consumer.

---

You're all set! Debezium is now streaming changes from Postgres into Kafka in real time.