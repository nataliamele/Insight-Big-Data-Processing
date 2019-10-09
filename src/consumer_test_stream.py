#!/usr/bin/env python
# coding: utf-8

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import (functions as F,
                         SparkSession,
                         SQLContext,
                         Row
                        )

from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition

import json

from pytz import timezone
from datetime import datetime

from sqlalchemy import create_engine
import psycopg2

import os
import set_environment

# call set_environment.py to set environment veriables  
# set_environ()

db_name = os.environ.get("db_name")
db_IP = os.environ.get("db_IP")
db_user = os.environ.get("db_user")
db_pwd = os.environ.get("db_pwd")
kafka_brokers = ['10.0.0.7:9092','10.0.0.9:9092','10.0.0.11:9092']

def observation(rdd):
    if rdd.isEmpty():
        print("Observation RDD is empty")
    else:     
        #set schema for dataframe
        schema = StructType([
                    StructField("ts", TimestampType()),
                    StructField("node_id", StringType()),
                    StructField("sensor_path", StringType()),
                    StructField("value_hrf", FloatType())])
        
        df = sql_context.createDataFrame(rdd, schema)
        # df = df.na.drop()
        
        df = df.withColumn("ts",  df["ts"])
        df = df.withColumn("node_id", (df["node_id"]))
        df = df.withColumn("sensor_path", df["sensor_path"])
        df = df.withColumn("value_hrf", df["value_hrf"])

        #once differenced na appears for percent_change column. drop those na's
        # df = df.na.drop()
        # df.show()

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

#setting spark connection to kafka and configs
sc = SparkContext(appName="SensorStreams").getOrCreate()
sc.setLogLevel("WARN")
#second argument is the micro-batch duration
ssc = StreamingContext(sc, 10)
spark = SparkSession(sc)
spark.conf.set("spark.sql.session.timeZone", "UTC")
sql_context = SQLContext(sc, spark)

# Processing sensor data coming from kafka #########################
read_topic = KafkaUtils.createStream(ssc, kafka_brokers, "spark-consumer", {"sensors_data": 1})

# JSON loads returns a dictionary
parsed_ = json.loads(read_topic)
parsed_ = parsed_.map(lambda w: (str(w["time"]), str(w["code"]), w["bid_price"], w["ask_price"]))
parsed_.pprint()
parsed_.foreachRDD(observation)
##################################################################

ssc.start()
ssc.awaitTermination()