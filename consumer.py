import findspark
findspark.init()
from kafka import KafkaConsumer
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
KAFKA_TOPIC = 'twitter_data'
KAFKA_BROKERS = 'localhost:9092'
ZOOKEEPER = 'localhost:2181'
sc = SparkContext('local[*]','test')
ssc = StreamingContext(sc, 60)
kafkaStream = KafkaUtils.createStream(ssc, ZOOKEEPER, 'spark-streaming',{KAFKA_TOPIC:1})
kafkaStream.pprint()
ssc.start()
ssc.awaitTermination()
