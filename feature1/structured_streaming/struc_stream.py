from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split


def struct_stream(spark):
	print "fun called"
	lines = spark.readStream.format("socket").option("host","localhost").option("port",9999).load()
	words = lines.select(explode(split(lines.values, " ")).alias("word"))
	wordCounts = words.groupBy("word").count()
	query = wordCounts.writeStream.outputMode("complete").format("console").start()
	query.awaitTermination()





if __name__=="__main__":
	spark = SparkSession.builder.appName("Basic structured Streaming").getOrCreate()
	struct_stream(spark)








