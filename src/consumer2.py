
#!/usr/bin/env python
# coding: utf-8

import sys
import operator
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def postgres_batch(df, epoch_id):
    df.write.jdbc(\
        url="jdbc:postgresql://10.0.0.4:5342/aotdb",\
        table="public.observations2",\
        mode="append",\
        properties={\
            "user": os.environ['DB_USER'],\
            "password": os.environ['DB_PWD']\
            }
        )
if __name__ == "__main__":

    #Kafka parameters
    zookeeper="10.0.0.7:2181,10.0.0.9:2181,10.0.0.11:2181"
    broker="10.0.0.7:9092,10.0.0.9:9092,10.0.0.11:9092"
    topic="sensors2"

    spark = SparkSession\
        .builder\
        .appName("SensorsStream")\
        .getOrCreate()

    # Create DataSet representing the stream of input lines from kafka
    # .option("zookeeper.connect", zookeeper)
    df = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", broker)\
        .option("subscribe", topic)\
        .option("startingOffsets","earliest") \
        .load()\
        .selectExpr("CAST(value AS STRING)")

    # Desired format of the incoming data
    df_schema = StructType([ StructField("ts", IntegerType())\
                                , StructField("node_id", StringType())\
                                , StructField("sensor_path", StringType())\
                                , StructField("value_hrf", FloatType())\
                             ])

    # Parse into a schema using Spark's JSON decoder:
    df_parsed = df.select(
            get_json_object(df.value, "$.ts").cast(IntegerType()).alias("ts"), \
            get_json_object(df.value, "$.node_id").cast(StringType()).alias("node_id"),\
            get_json_object(df.value, "$.sensor_path").cast(StringType()).alias("sensor_path"),\
            get_json_object(df.value, "$.value_hrf").cast(FloatType()).alias("value_hrf")\
            )

    # print_console = df_parsed.writeStream \
    #     .outputMode('update')\
    #     .format('console')\
    #     .start()

    # print_console.awaitTermination()

    ## write to TimescaleDB 

    write_db = df_parsed.writeStream \
           .outputMode("append") \
           .foreachBatch(postgres_batch) \
           .start()\
           .awaitTermination()