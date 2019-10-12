'''
Reads Producer's data, filter and store to db
'''

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import operator
# import numpy as np
import os

# foreachBatch write sink; helper function for writing streaming dataFrames
def postgres_batch(df, epoch_id):
    df.write.jdbc(
        url="jdbc:postgresql://10.0.0.4:5342/aotdb",
        table="public.observations",
        mode="append",
        properties={
            "user": os.environ['PG_USER'],
            "password": os.environ['PG_PWD']
            }
        )

if __name__ == "__main__":

    # Create SparkSession
    spark = SparkSession \
        .builder \
        .appName("SensorsDataStream") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Desired format of the incoming data
    dfSchema = StructType([ StructField("ts", IntegerType())\
                                , StructField("node_id", StringType())\
                                , StructField("sensor_path", StringType())\
                                , StructField("value_hrf", FloatType())\
                             ])

    # Subscribe to a Kafka topic
    dfstream = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers","10.0.0.7:9092,10.0.0.9:9092,10.0.0.11:9092") \
        .option("subscribe", "sensors-data") \
        .option("startingOffsets","earliest") \
        .load() 

    dfstream.printSchema() 
    dfstream_str=dfstream.selectExpr("CAST(value AS STRING)")       

    # Parse this into a schema using Spark's JSON decoder:
    df_parsed = dfstream_str.select(
            get_json_object(dfstream_str.value, "$.ts").cast(IntegerType()).alias("ts"), \
            get_json_object(dfstream_str.value, "$.node_id").cast(StringType()).alias("node_id"),\
            get_json_object(dfstream_str.value, "$.sensor_path").cast(StringType()).alias("sensor_path"),\
            get_json_object(dfstream_str.value, "$.value_hrf").cast(FloatType()).alias("value_hrf")\
            )
    
    # write to console

    # consoleOutput = df_parsed.writeStream \
    # .format("console") \
    # .trigger(once=True) \
    # .start() \
    # .awaitTermination()

    ## write to TimescaleDB 

    df_write = df_parsed.writeStream \
            .outputMode("append") \
            .foreachBatch(postgres_batch) \
            .start()\
            .awaitTermination()

