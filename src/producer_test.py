from kafka.producer import KafkaProducer
import sys
from aot_client import AotClient, F
import ciso8601
import time
from json import dumps

topic = "sensors-data"
brokers = ['10.0.0.7:9092','10.0.0.9:9092','10.0.0.11:9092']

# Instantiate a Kafka Producer

producer = KafkaProducer(bootstrap_servers=brokers, \
                         value_serializer=lambda x: \
                         dumps(x).encode('utf-8'))

message = {"ts": 1570006294, "node_id": "06B", "sensor_path": "lightsense.tmp421.temperature", "value_hrf": 24.38}

producer.send(topic, value=data_stream)
producer.flush()
