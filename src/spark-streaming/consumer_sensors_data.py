
'''
Reads Producer's data, filters and stores to data base
'''

import os
#    Spark
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
#    json parsing
import json


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, FloatType
from pyspark.sql.functions import *
from pyspark.sql.types import *

from sqlalchemy import create_engine
import psycopg2

#   Create Spark context
sc = SparkContext(appName="SensorData_Processing")
sc.setLogLevel("WARN")

#   Create Streaming context
ssc = StreamingContext(sc, 60)

#   Set streaming context checkpoint directory, for the use of Window function to count number of observations eg in an hour
ssc.checkpoint('home/ubuntu/batch/sensor-data/')

#   Using streaming context from above, connect to Kafka cluster
sensor_data = KafkaUtils.createStream(
                                    ssc, 
                                    kafka_brokers, 
                                    'spark-streaming', {'sensors-data':3})

#   Parse JSON from inbound DStream

parsed_observation = kafkaStream.map(lambda v: json.loads(v[1]))

# Stdout number of recors
parsed_observation.count().map(lambda x:'records in this batch: %s' % x).pprint()

def observation(rdd):
    if rdd.isEmpty():
        print("Observation RDD is empty")
    else:     
        #set schema for dataframe
        schema = StructType([
                    StructField("ts", TimestampType()),
                    StructField("node_id", StringType()),
                    StructField("sensor_path", StringType()),
                    StructField("value", FloatType())])
        
        df = sql_context.createDataFrame(rdd, schema)
        
        df = df.withColumn("ts",  df["ts"])
        df = df.withColumn("node_id", (df["node_id"]))
        df = df.withColumn("sensor_path", df["sensor_path"])
        df = df.withColumn("value_hrf", df["value"])
        add_to_db(df, "observations")
        # detect_extreme_val(df)
def add_to_db(df, table_name):
    df = df.toPandas()
    create_engine("postgresql://{}:{}@{}:5342/{}".format(db_user, db_name, db_IP, db_name))
    df.head(0).to_sql(table_name, engine, if_exists='append',index=False) #truncates the table
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, table_name, null="") # null values become ''
    conn.commit()

#incoming stock data is messy check whether the important fields are present on a given quote, if not map to a tuple of none
parsed_observation.pprint()
parsed_observation.foreachRDD(observation)

# Start streaming context
ssc.start()
ssc.awaitTermination # (timeout=180) # add timeout for termination during testing

if __name__ == "__main__":

    db_name = os.environ.get("db_name")
    db_IP = os.environ.get("db_IP")
    db_user = os.environ.get("db_user")
    db_pwd = os.environ.get("db_pwd")
    kafka_brokers = ['10.0.0.7:9092','10.0.0.9:9092','10.0.0.11:9092']