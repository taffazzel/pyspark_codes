from pyspark.sql import Row
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession

def rddtodataframe():
	appName = "RDDTODF"
	master = "local[*]"
	conf = SparkConf().setAppName(appName).setMaster(master)
	sc = SparkContext(conf = conf)
	lines = sc.textFile("file:///root/testRDD.txt")
	parts = lines.map(lambda l: l.split(","))
	people = parts.map(lambda s:Row(name = s[0],age = int(s[1])))
	print people
	spark = SparkSession.builder.appName("BA").getOrCreate()
	schemaPeople = spark.createDataFrame(people)
	schemaPeople.createOrReplaceTempView("Peoples")
	teen = spark.sql("select * from peoples where age >= 13 and age<= 19")
	teenNames = teen.rdd.map(lambda p:"Name:" +p.name).collect()
	for i in teenNames:
		print i

if __name__ == "__main__":
	print "PRINT"
	#spark = SparkSession.builder.appName("BASIC").getOrCreate()
	rddtodataframe()
