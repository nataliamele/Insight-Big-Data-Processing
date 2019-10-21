
#!/usr/bin/env python
# coding: utf-8

import sys
import operator
import os

import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import udf, col, to_timestamp, round
from pyspark.sql.types import *

def read_from_db(table_name):
    '''
    Connects to Postgres and reads table into df.
    To use your own schema use .schema(schema) instead of ' .option("inferSchema", "true") ' and add to to func parameters
    Returns df
    '''
    df = spark.read\
        .format("jdbc")\
        .option("inferSchema", "true") \
        .option("header", "true") \
        .options(\
            driver="org.postgresql.Driver", \
            url="jdbc:postgresql://10.0.0.4:5342/aotdb", \
            dbtable=table_name,user=os.environ['DB_USER'], password=os.environ['DB_PWD'])\
        .load()
    return df

def postgres_batch(df, epoch_id):
    df.write.jdbc(\
        url="jdbc:postgresql://10.0.0.4:5342/aotdb",\
        table="public.observations",\
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
    topic="sensors-data"

    spark = SparkSession\
        .builder\
        .appName("SensorsStream")\
        .getOrCreate()

    # schema_sensors = StructType([ StructField("sensor_path", StringType())\
    #                             , StructField("node_id", StringType())\
    #                             , StructField("sensor_path", StringType())\
    #                             , StructField("value_hrf", FloatType())\
    #                          ])   

    # Create sensors dataframe from Postgres table
    df_sensors = read_from_db('public.sensors')\
        .select('sensor_path','sensor_measure','hrf_unit','hrf_max')
    
    # Create nodes dataframe    
    df_nodes = read_from_db('public.nodes')\
        .select('vsn','lat', 'lon', 'community_area')\
        .withColumn('lat', round(col('lat'), 2))\
        .withColumn('lon', round(col('lon'), 2))

    # Connect to Kafka and load stream
    # .option("zookeeper.connect", zookeeper)
    df = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", broker)\
        .option("subscribe", topic)\
        .option("startingOffsets","latest") \
        .load()\
        .selectExpr("CAST(value AS STRING)")

    # Design schema format for incoming data
    # df_schema = StructType([ StructField("ts", IntegerType())\
    #                             , StructField("node_id", StringType())\
    #                             , StructField("sensor_path", StringType())\
    #                             , StructField("value_hrf", FloatType())\
    #                          ])

    # Parse into a schema using Spark's JSON decoder:
    df_parsed = df.select(
            get_json_object(df.value, "$.ts").cast(IntegerType()).alias("ts"), \
            get_json_object(df.value, "$.node_id").cast(StringType()).alias("node_id"),\
            get_json_object(df.value, "$.sensor_path").cast(StringType()).alias("sensor_path"),\
            get_json_object(df.value, "$.value_hrf").cast(FloatType()).alias("value_hrf")\
            )

    # Filter Chemsense and Aphasense sensors
    df_obs = df_parsed \
        .filter((col('sensor_path').like('chemsense.%')) | (col('sensor_path').like('alphasense.opc_n2.pm%')) )\
        .select('ts','node_id','sensor_path','value_hrf')\
        .withColumn('value_hrf', round(col('value_hrf'), 2))

    # Enreach observation streaming df
    df_result= df_obs.join(df_nodes, df_obs.node_id == df_nodes.vsn, 'left_outer')\
        .join(df_sensors, df_obs.sensor_path == df_sensors.sensor_path, 'left_outer').drop(df_sensors.sensor_path) \
        .select('ts','vsn','sensor_path','value_hrf','hrf_unit','lat', 'lon', 'community_area')


    ## write to TimescaleDB 

    write_db = df_result.writeStream \
           .outputMode("append") \
           .foreachBatch(postgres_batch) \
           .start()\
           .awaitTermination()