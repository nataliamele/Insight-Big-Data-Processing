import sys
import operator
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def read_from_db(db_name):

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

if __name__ == "__main__":

    # sensors_csv = "s3://insight-natnm/data/sensors.csv"
    # nodes_csv = "s3://insight-natnm/data/nodes.csv"

    spark = SparkSession\
            .builder\
            .appName('ReadPG')\
            .getOrCreate()


    df_sensors = read_from_db(public.sensors)\
        .select( 'sensor_path','sensor_measure','hrf_unit','hrf_max')
    
    df_obs = read_from_db(public.sensors)\
        .select('ts','node_id','sensor_path','value_hrf')
    
    df_nodes = read_from_db(public.sensors)\
        .select('node_id','vsn','lat', 'lon', 'community_area')
    
    df_result= df_obs.join(df_nodes,df_obs("node_id") == df_nodes("vsn"), how ='left')\
        .select('ts','node_id','sensor_path','value_hrf','lat', 'lon', 'community_area')

    df_result.show().take(10)
    # df_nodes.take(10).show()
    