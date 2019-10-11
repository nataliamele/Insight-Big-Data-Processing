# Run spark job - consumer

/usr/local/spark/bin/spark-submit --master spark://10.0.0.8:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 --driver-class-path /usr/local/spark/jars/ --jars /usr/local/spark/jars/postgresql-42.2.8.jarpostgresql-42.2.8.jar /home/ubuntu/City-Health-Monitor/src/consumer.py
    