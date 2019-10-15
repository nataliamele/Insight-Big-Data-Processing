'''
Reads Producer's data, filter and store to db
'''
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import operator
import os

def read_kafka_stream(topic, broker):
    '''
    Reads stream from Kafka
    Assign schema 
    Returns parsed dataframe
    '''

    # Desired format of the incoming data
    df_schema = StructType([ StructField("ts", IntegerType())\
                                , StructField("node_id", StringType())\
                                , StructField("sensor_path", StringType())\
                                , StructField("value_hrf", FloatType())\
                             ])

    df = spark.read \
    .format("kafka") \
    .option("zookeeper.connect", zookeeper)
    .option("kafka.bootstrap.servers", broker) \
    .option("subscribe", topic) \
    .load()
    .select(from_json(col("value").cast("string"), df_schema).alias("parsed_value")

    df_json = df.select("parsed_value.ts","parsed_value.node_id","parsed_value.sensor_path","parsed_value.value_hrf")
    
    return df_json

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

def write_stream(df):
    # write to console

    #consoleOutput = dfstream.writeStream \
    # .outputMode("update") \
    # .format("console") \
    # .start() \
    # .awaitTermination()

    #.trigger(once=True) \

    ## write to TimescaleDB 

    df_write = df_parsed.writeStream \
            .outputMode("append") \
            .foreachBatch(postgres_batch) \
            .start()\
            .awaitTermination()

if __name__ == "__main__":

    spark = SparkSession.builder \
            .appName("SensorsDataStream") \
            .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    #Kafka parameters
    zookeeper="zookeeper1:2181,zookeeper2:2181,zookeeper3:2181"
    broker="kafka1:9092,kafka2:9092,kafka3:9092"
    topic="observationsstream"

    #Consume the data from Kafka and process it
    
    write_stream(read_kafka_stream(topic,broker))