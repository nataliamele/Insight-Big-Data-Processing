
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
                                    'spark-streaming', {'sensors-data':3})

#   Parse JSON from inbound DStream

# parsed_observation = kafkaStream.map(lambda v: json.loads(v[1]))
parsed_observation = json.loads(sensor_data)

# Stdout number of records
parsed_observation.count().map(lambda x:'records in this batch: %s' % x).pprint()

# sql = observations_table_insert
observations_table_insert = "INSERT INTO public.observations (ts, node_id, sensor_path, value_hrf) VALUES (%s,%s,%s,%s)"

to_insert = list(parsed_observation.values())

try:
    cur.executemany(observations_table_insert, to_insert)
    conn.commit()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)

# def observation(rdd):
#     if rdd.isEmpty():
#         print("Observation RDD is empty")
#     else:     
#         #set schema for dataframe
#         schema = StructType([
#                     StructField("ts", TimestampType()),
#                     StructField("node_id", StringType()),
#                     StructField("sensor_path", StringType()),
#                     StructField("value", FloatType())])
        
#         df = sql_context.createDataFrame(rdd, schema)
        
#         df = df.withColumn("ts",  df["ts"])
#         df = df.withColumn("node_id", (df["node_id"]))
#         df = df.withColumn("sensor_path", df["sensor_path"])
#         df = df.withColumn("value_hrf", df["value"])
#         add_to_db(df, "observations")
#         # detect_extreme_val(df)
# def add_to_db(df, table_name):
#     create_engine("postgresql://{}:{}@{}:5342/{}".format(db_user, db_name, db_IP, db_name))
#     df.head(0).to_sql(table_name, engine, if_exists='append',index=False) #truncates the table
#     conn = engine.raw_connection()
#     cur = conn.cursor()
#     output = io.StringIO()
#     df.to_csv(output, sep='\t', header=False, index=False)
#     output.seek(0)
#     contents = output.getvalue()
#     cur.copy_from(output, table_name, null="") # null values become ''
#     conn.commit()

parsed_observation.pprint()
# parsed_observation.foreachRDD(observation)

# Start streaming context
ssc.start()
ssc.awaitTermination # (timeout=180) # add timeout for termination during testing

if __name__ == "__main__":
    # Connect to DB
    conn = psycopg2.connect(host="10.0.0.4", port="", database="", user="", password="")
    cur = conn.cursor()
    # set_environ()

    # db_name = os.environ.get("DB_NAME")
    # db_IP = os.environ.get("DB_IP")
    # db_user = os.environ.get("DB_USER")
    # db_pwd = os.environ.get("DB_PWD")
    kafka_brokers = ['10.0.0.7:9092','10.0.0.9:9092','10.0.0.11:9092']