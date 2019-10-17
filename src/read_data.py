import sys
import operator
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":

    sensors_csv = "s3://insight-natnm/data/sensors.csv"
    nodes_csv = "s3://insight-natnm/data/nodes.csv"

    spark = SparkSession\
            .builder\
            .appName('ReadPG')\
            .getOrCreate()

    df_sensors = spark \
        .read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("charset", "UTF-8")\
        .csv(sensors_csv)

    df_nodes = spark \
        .read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("charset", "UTF-8")\
        .csv(nodes_csv)
    
    df_sensors.take(10).show()
    df_nodes.take(10).show()