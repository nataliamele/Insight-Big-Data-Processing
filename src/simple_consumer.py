#    Spark
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
#    json parsing
import json

sc = SparkContext(appName="PythonSparkStreaming")
sc.setLogLevel("WARN")

ssc = StreamingContext(sc, 60)

kafkaStream = KafkaUtils.createStream(ssc, '10.0.0.7:9092, 10.0.0.9:9092, 10.0.0.11:9092', 'spark-streaming', {'observationsstream':1})

parsed = kafkaStream.map(lambda v: json.loads(v[1]))

parsed.pprint()

ssc.start()
ssc.awaitTermination()