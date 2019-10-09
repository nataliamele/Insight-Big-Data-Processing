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
def postgres_batch(df, epoch_id):
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
    dfSchema = StructType([ StructField("ts", TimestampType(), True)\
                                , StructField("node_id", StringType(),True)\
                                , StructField("sensor_path", StringType(), True)\
                                , StructField("value_hrf", FloatType(), True)\
                             ])
    # Subscribe to a Kafka topic
    dfstream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers",
                "10.0.0.7:9092,10.0.0.9:9092,10.0.0.11:9092") \
        .option("subscribe", "sensors-data") \
        .load() \
        .select(from_json(col("value").cast("string"), dfSchema).alias("parsed_value"))
    
    dfstream.printSchema()

    # dfstream_str = dfstream.selectExpr("CAST(value AS STRING)")
    # Parse this into a schema using Spark's JSON decoder:
    # df_parsed = dfstream_str.select(
    #         get_json_object(dfstream_str.value, "$.ts").cast(TimestampType()).alias("ts"),
    #         get_json_object(dfstream_str.value, "$.node_id").cast(StringType()).alias("node_id")
    #         get_json_object(dfstream_str.value, "$.sensor_path").cast(StringType()).alias("sensor_path")
    #         get_json_object(dfstream_str.value, "$.value_hrf_id").cast(FloatType()).alias("value_hrf")
    # )
    print('Dfstream:', dfstream)
    df_parsed = dfstream.select(\
        "parsed_value.ts",\
        "parsed_value.node_id",\
        "parsed_value.sensor_path",\
        "parsed_value.value_hrf",\
        )

    print("observed_data_parsed",df_parsed)
    # DataFrame[ts: timestamp, node_id: string, sensor_path: string, value_hrf: float])

    #write to TimescaleDB 
    df_write = df_parsed\
        .writeStream \
        .outputMode("append") \
        .foreachBatch(postgres_batch) \
        .start()

    df_write.awaitTermination()

