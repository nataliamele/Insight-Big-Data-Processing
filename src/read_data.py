import sys
import operator
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":

    # sensors_csv = "s3://insight-natnm/data/sensors.csv"
    # nodes_csv = "s3://insight-natnm/data/nodes.csv"

    spark = SparkSession\
            .builder\
            .appName('ReadPG')\
            .getOrCreate()


    df_sensors = spark.read\
          .format("jdbc")\
          .options(\
              driver="org.postgresql.Driver", \
              url="jdbc:postgresql://10.0.0.4:5342/aotdb", \
              dbtable="public.sensors",user=os.environ['DB_USER'], password=os.environ['DB_PWD'])\
          .load()
    
    df_obs = spark.read\
          .format("jdbc")\
          .options(\
              driver="org.postgresql.Driver", \
              url="jdbc:postgresql://10.0.0.4:5342/aotdb", \
              dbtable="public.observations2",user=os.environ['DB_USER'], password=os.environ['DB_PWD'])\
          .load()
    
    df_nodes = spark.read\
          .format("jdbc")\
          .options(\
              driver="org.postgresql.Driver", \
              url="jdbc:postgresql://10.0.0.4:5342/aotdb", \
              dbtable="public.nodes",user=os.environ['DB_USER'], password=os.environ['DB_PWD'])\
          .load()
    
    df_sensors.show()
    df_obs.show().take(10)
    # df_nodes.take(10).show()
    