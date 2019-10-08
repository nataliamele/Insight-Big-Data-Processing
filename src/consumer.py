
'''
Reads Producer's data, filters and stores to db
'''

import os
#    Spark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, FloatType
from pyspark.sql.functions import *

#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
#    json parsing
import json

# from sqlalchemy import create_engine
import psycopg2
from sql_queries import *

# import set_environment

#   Create Spark context
sc = SparkContext(appName="SensorData_Processing")
sc.setLogLevel("WARN")

#   Create Streaming context
ssc = StreamingContext(sc, 60)

#   Set streaming context checkpoint directory, for the use of Window function to count number of observations eg in an hour
ssc.checkpoint('home/ubuntu/batch/sensor-data/')

#   Using streaming context from above, connect to Kafka cluster
sensor_data = KafkaUtils.createStream(\
                                    ssc, \
                                    kafka_brokers, \
                                    'spark-streaming', {'sensors-data':1})

#   Parse JSON from inbound DStream

parsed_observation = json.loads(sensor_data)

# Stdout number of records
parsed_observation.count().map(lambda x:'records in this batch: %s' % x).pprint()

# insert observation to db, sql statement
observations_table_insert = "INSERT INTO public.observations (ts, node_id, sensor_path, value_hrf) VALUES (%s,%s,%s,%s)"

# values from stream to insert 
to_insert = list(parsed_observation.values())

try:
    cur.executemany(observations_table_insert, to_insert)
    conn.commit()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)


parsed_observation.pprint()
# parsed_observation.foreachRDD(observation)

# Start streaming context
ssc.start()
ssc.awaitTermination # (timeout=180) # add timeout for termination during testing

if __name__ == "__main__":
    # Connect to DB
    conn = psycopg2.connect(host="10.0.0.4", port="", database="", user="", password="")
    cur = conn.cursor()
    kafka_brokers = ['10.0.0.7:9092','10.0.0.9:9092','10.0.0.11:9092']