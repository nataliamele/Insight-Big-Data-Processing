from __future__ import print_function
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
import pyspark.sql.functions as func
from pyspark.sql import DataFrameReader
from pyspark.sql.types import FloatType, BooleanType, IntegerType
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import StructType, IntegerType, StringType, FloatType, StructField

import os
import sys
import time
import json
import pandas as pd

from sqlalchemy import create_engine


databaseName = "aotdb"
databaseIP = "10.0.0.4:5342"
databaseUser = os.environ.get("PG_USER")
databasePassword = os.environ.get("PG_PWD")
kafkaIPZooPort = "10.0.0.7:9092,10.0.0.9:9092,10.0.0.11:9092"


# def obs_rdd(rdd):
#     if rdd.isEmpty():
#         print("observation RDD is empty")
#     else:
#         # df = rdd.toDF()
#         # df = df.selectExpr("ts", "node_id", "sensor_path", "value_hrf")
#         # df = df.withColumn("ts",df["ts"])
#         # df = df.withColumn("node_id",df["node_id"])
#         # df = df.withColumn("sensor_path",df["sensor_path"])
#         # df = df.withColumn("value_hrf",df["value_hrf"])
       
#         df = df.na.drop()
#         df = df.select(["ts", "node_id", "sensor_path", "value_hrf"])

#         addToDB(df, "public.observations")


def obs_rdd(rdd):
    if rdd.isEmpty():
        print("Observation RDD is empty")
    else:
      
        #set schema for dataframe
        schema = StructType([
                    StructField("ts", IntegerType()),
                    StructField("node_id", StringType()),
                    StructField("sensor_path", StringType()),
                    StructField("value_hrf", FloatType())])

        df = sql_context.createDataFrame(rdd, schema)
        df.show()

        addToDB(df, "public.observations")


#stores the dataframe into the given table in the database
def addToDB(df, table):
    df = df.toPandas()
    engine = create_engine("postgresql://{}:{}@{}:5432/{}".format(databaseUser, databaseName, databaseIP, databaseName))
    df.head(0).to_sql(table, engine, if_exists='append',index=False) #truncates the table
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, table, null="") # null values become ''
    conn.commit()



#setting spark connection to kafka and configs
sc = SparkContext(appName="SensorDataStream").getOrCreate()
sc.setLogLevel("WARN")
#second argument is the micro-batch duration
ssc = StreamingContext(sc, 10)
spark = SparkSession(sc)
spark.conf.set("spark.sql.session.timeZone", "UTC")
broker = kafkaIPZooPort
sql_context = SQLContext(sc, spark)


#processing forex data coming from kafka #########################
input_obs = KafkaUtils.createStream(ssc, kafkaIPZooPort, "spark-consumer", {"sensors-data": 1})
#json loads returns a dictionary
parsed_obs = input_obs.map(lambda v: json.loads(v[1]))
parsed_obs = parsed_obs.map(lambda w: (str(w["ts"]), str(w["node_id"]), w["sensor_path"], w["value_hrf"]))
parsed_obs.pprint()
parsed_obs.foreachRDD(obs_rdd)
##################################################################

ssc.start()
ssc.awaitTermination()