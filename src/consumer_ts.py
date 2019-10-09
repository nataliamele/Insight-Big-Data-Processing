from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import operator
import numpy as np
import os


spark = SparkSession \
            .builder \
            .appName("Spark_test") \
            .getOrCreate()

# actually works?
spark.sparkContext.setLogLevel("ERROR")

brokers = "10.0.0.7:9092,10.0.0.9:9092,10.0.0.11:9092"
topic = "sensors-data"

dfSchema = StructType([ StructField("ts", TimestampType()), \
                        StructField("node_id", StringType()),\
                        StructField("sensor_path", StringType())\
                        StructField("value_hrf", FloatType())\
                        ])
    
# schema = StructType()\
#     .add('post',StringType())\
#     .add('subreddit',StringType())\
#     .add('timestamp',TimestampType())


input_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", brokers) \
  .option("subscribe", topic) \
  .option("startingOffsets", "earliest") \
  .load()


input_df.printSchema()

trans_df = input_df.selectExpr("CAST(value AS STRING)")
trans_df.show()

consoleOutput = input_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

consoleOutput.awaitTermination()


spark.streams.awaitAnyTermination()