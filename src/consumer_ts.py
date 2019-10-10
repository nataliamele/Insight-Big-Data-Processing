from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(appName="PythonSparkStreamingKafka")
sc.setLogLevel("WARN")

ssc = StreamingContext(sc,60)

kafkaStream = KafkaUtils.createStream(ssc, '10.0.0.7:9092,10.0.0.9:9092,10.0.0.11:9092', 'spark-stream', {'sensors-data':1})
lines = kafkaStream.map(lambda x: x[1])
lines.pprint()

ssc.start()  
ssc.awaitTermination()