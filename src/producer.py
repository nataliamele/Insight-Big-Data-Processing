from kafka.producer import KafkaProducer
import sys
from aot_client import AotClient, F
import ciso8601
import time
import datetime
from time import sleep
from json import dumps

topic = "sensorsdata"
brokers = ['10.0.0.7:9092','10.0.0.9:9092','10.0.0.11:9092']

# Instantiate a Kafka Producer

producer = KafkaProducer(bootstrap_servers=brokers, \
                         value_serializer=lambda x: \
                         dumps(x).encode('utf-8'))

# Initialize AoT client
client = AotClient()

# Get previous record timestamp 
try:
    fh = open("state.txt", "r")
    prev_record_timestamp = fh.read()
    t = ciso8601.parse_datetime(prev_record_timestamp)
    #t=datetime.datetime.strptime(prev_record_timestamp, '%Y-%m-%dT%H:%M:%S')
    fh.close()
except FileNotFoundError:
    # t = (datetime.datetime.utcnow() - datetime.timedelta(hours=6))
    t = (datetime.datetime.utcnow() - datetime.timedelta(minutes=15))
    prev_record_timestamp = t.isoformat()[0:19]

# Initialize filter (city- Chicago, 5000 records, timestamp, order by timestamp)
f = F('project', 'chicago')
f &= ('size', '1000')
f &= ('timestamp', 'gt', prev_record_timestamp)
f &= ('order', 'asc:timestamp')

# Get observations from AoT
observations = client.list_observations(filters=f)
# Iterate through records
for page in observations:
    for obs in page.data:
        ts = ciso8601.parse_datetime(obs["timestamp"])
        prev_record_timestamp = obs["timestamp"]
        data_stream = {
                        'ts': int(time.mktime(ts.timetuple())),\
                        'node_id': obs["node_vsn"],\
                        'sensor_path': obs["sensor_path"],\
                        'value_hrf': obs["value"]\
                        }
        producer.send(topic, value=data_stream)

    # Block until all the messages have been sent
    producer.flush()
fh = open("state.txt", "w+")
fh.write(prev_record_timestamp)
fh.close()