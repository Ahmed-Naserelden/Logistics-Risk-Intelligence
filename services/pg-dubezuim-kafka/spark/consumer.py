# ths code for testing 

from confluent_kafka import Consumer, KafkaException

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'iti_group',
    'auto.offset.reset': 'earliest'
}

my_topic = 'maritime_logistics_server.public.seismic_events'

consumer = Consumer(conf)

consumer.subscribe([my_topic])

try:
    cnt = 0
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            cnt += 1
            print(f"[Message {cnt}]", "#"*90)
            print(f"Received message: {msg.value().decode('utf-8')} (key: {msg.key()})")
            print(" " * (len(str(cnt)) + 10), "#"*90)
            print()
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
