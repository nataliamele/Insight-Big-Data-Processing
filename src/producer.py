from kafka.producer import KafkaProducer
import sys
from aot_client import AotClient, F
import ciso8601
import time
import datetime
from time import sleep
from json import dumps
import psutil
import os


def process_is_running(process_name):
    # Iterate over the all the running process
    for proc in psutil.process_iter():
        try:
            # Check if process name contains the given name string.
            if process_name.lower() in proc.cmdline() and proc.pid != os.getpid():
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return False


if process_is_running('producer.py'):
    print('Found another instance, exiting...')
    exit()

topic = "obs-stream"
brokers = ['10.0.0.7:9092','10.0.0.9:9092','10.0.0.11:9092']
mins_ago=60 #*6

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
    fh.close()
except FileNotFoundError:
    t = (datetime.datetime.utcnow() - datetime.timedelta(minutes=mins_ago))
    prev_record_timestamp = t.isoformat()[0:19]

# Initialize filter (city- Chicago, 5000 records, timestamp, order by timestamp)
f = F('project', 'chicago')
f &= ('size', '5000')
f &= ('timestamp', 'gt', prev_record_timestamp)
f &= ('order', 'asc:timestamp')

# Set counter for retrieved pages 
page_num = 1

# Get observations from AoT
observations = client.list_observations(filters=f)

# Iterate through records
try:
    for page in observations:
        print(f'Page {page_num}')
        # data_stream = []
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
        page_num += 1

except (Exception, HTTPError) as error:
    print(error)
finally:
    # Write latest processed timestamp to the file  
    fh = open("state.txt", "w+")
    fh.write(prev_record_timestamp)
    print(prev_record_timestamp)
    fh.close()