from pyspark.sql import Row
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

def rating():
	conf = SparkConf().setAppName("MostRatedMovies")
	sc = SparkContext(conf = conf)
	lineRdd = sc.textFile("file:///home/hdfs/ml-20m/smallrating.csv")
	#print "*******************************",t.count()
	header = lineRdd.first()
	frame = lineRdd.filter(lambda s:s!=header).map(lambda l:l.split(",")).map(lambda p:Row(userid = p[0],movieid = p[1], rating = float(p[2]), timestamp = p[2]))
	spark = SparkSession.builder.appName("rating").getOrCreate()
	schemaRating = spark.createDataFrame(frame).cache()
	schemaRating.createOrReplaceTempView("ratings")
	query = "select * from ratings limit 10"
	spark.sql(query).show()
	schemaRating.select(schemaRating['userid']).show()
	#view_rates = top.rdd.map(lambda p:p.rating).collect()
	#for t in view_rates:
		#print t

	#spark.stop()

if __name__=="__main__":
	rating()
