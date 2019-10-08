
'''
Reads Producer's data, filter and store to db
'''

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import operator
import numpy as np
# import pywt
# import entropy
import os

# foreachBatch write sink; helper function for writing streaming dataFrames
def postgres_batch_analyzed(df, epoch_id):
    df.write.jdbc(
        url="jdbc:postgresql://10.0.0.4:5342/aotdb",
        table="observations",
        mode="append",
        properties={
            "user": os.environ['PG_USER'],
            "password": os.environ['PG_PWD']
        })


if __name__ == "__main__":
    # Create a local SparkSession
    # https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#quick-example
    spark = SparkSession \
        .builder \
        .appName("CityHealthMonitor") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    # Desired format of the incoming data
    dfSchema = StructType([ StructField("ts", TimestampType(), False)\
                                , StructField("node_id", StringType(),False)\
                                , StructField("sensor_path", StringType(), False)\
                                , StructField("value_hrf", FloatType(), False)\
                             ])
    # Subscribe to a Kafka topic
    dfstream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers",
                "10.0.0.7:9092,10.0.0.9:9092,10.0.0.11:9092") \
        .option("subscribe", "sensors-data") \
        .load()\
        .select(from_json(col("value").cast("string"), dfSchema).alias("parsed_value"))

    # Parse this into a schema using Spark's JSON decoder:

    df_parsed = dfstream.select(\
        "parsed_value.ts",\
        "parsed_value.node_id",\
        "parsed_value.sensor_path",\
        "parsed_value.value_hrf",\
        )

    print("observed_data_parsed",df_parsed)

    #write to TimescaleDB 
    df_write = df_parsed\
        .writeStream \
        .outputMode("append") \
        .foreachBatch(postgres_batch_analyzed) \
        # .trigger(processingTime="5 seconds") \
        .start()

    df_write.awaitTermination()

