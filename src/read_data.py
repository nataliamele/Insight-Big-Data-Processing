import sys
import operator
import os

import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, round
from pyspark.sql.types import *



def read_from_db(db_name):
    '''
    Connects to Postgres and reads table into df
    Returns df
    '''
    df = spark.read\
        .format("jdbc")\
        .option("header", "true") \
        .option("inferSchema", "true") \
        .options(\
            driver="org.postgresql.Driver", \
            url="jdbc:postgresql://10.0.0.4:5342/aotdb", \
            dbtable=db_name,user=os.environ['DB_USER'], password=os.environ['DB_PWD'])\
        .load()
    return df

def read_from_s3(path):
    '''
    Connects to S3 and reads csv into df
    Returns df
    '''
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("charset", "UTF-8") \
        .csv(path)
    return df


if __name__ == "__main__":

    # sensors_csv = "s3://insight-natnm/data/sensors.csv"
    # nodes_csv = "s3://insight-natnm/data/nodes.csv"

    spark = SparkSession\
        .builder\
        .appName('ReadPG')\
        .getOrCreate()

    # Sensors dataframe
    df_sensors = read_from_db('public.sensors')\
        .select('sensor_path','sensor_measure','hrf_unit','hrf_max')
    # df_sensors.show()

    # df_sensors = read_from_s3("s3://insight-natnm/data/sensors.csv")\
    #     .select('sensor_path','sensor_measure','hrf_unit','hrf_max')
    # df_sensors.show()

    # Nodes dataframe
    
    df_nodes = read_from_db('public.nodes')\
        .select('vsn','lat', 'lon', 'community_area')\
        .withColumn('lat', round(col('lat'), 2))\
        .withColumn('lon', round(col('lon'), 2))

    # df_nodes.show()

    # df_nodes = read_from_s3("s3://insight-natnm/data/nodes.csv")\
    #     .select('node_id','vsn','lat', 'lon', 'community_area')
    # df_nodes.show()

    # Observations dataframe 

    df_obs = read_from_db('public.observations2')\
        .filter(col('sensor_path').contains.like('chemsense.%'))\
        .select('ts','node_id','sensor_path','value_hrf')\
        .withColumn('value_hrf', round(col('value_hrf'), 2))
    # df_obs.show()

    # Enreach observation dataframe
    df_result= df_obs.join(df_nodes, df_obs.node_id == df_nodes.vsn, how ='left')\
        .select('ts','node_id','sensor_path','value_hrf','lat', 'lon', 'community_area')
 
    df_result.show()
    # df_nodes.take(10).show()
    