# Run spark job - consumer

/usr/local/spark/bin/spark-submit \
	--master spark://10.0.0.12:7077 \
	--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 \
	--driver-class-path /usr/local/spark/jars/postgresql-42.2.8.jar \
	--jars /usr/local/spark/jars/postgresql-42.2.8.jar \
	--driver-memory 4G \
	--executor-memory 10G \
	/home/ubuntu/City-Health-Monitor/src/consumer.py