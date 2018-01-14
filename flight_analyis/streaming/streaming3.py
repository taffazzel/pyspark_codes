from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    
    sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    ssc = StreamingContext(sc, 1)

    #zkQuorum, topic = sys.argv[1:]
    zk = "localhost:2181"
    topic = "airport"
    kvs = KafkaUtils.createStream(ssc, zk, "spark-streaming-consumer", {topic: 1})
    lines = kvs.map(lambda x: x[1])
    #counts = lines.flatMap(lambda line: line.split(" ")) \
        #.map(lambda word: (word, 1)) \
        #.reduceByKey(lambda a, b: a+b)
    l = lines.filter(lambda s: s[0]!=0)
    l.foreachRDD(lambda s:s.split())
    ssc.start()
    ssc.awaitTerminationOrTimeout(10)
