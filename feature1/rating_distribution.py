from pyspark import SparkContext, SparkConf
import collections

conf = SparkConf().setMaster("local").setAppName("MovieRating")
sc = SparkContext(conf = conf)
fiRDD = sc.textFile("file:///home/hdfs/ml-20m/smallrating.csv")
header = fiRDD.first()
body = fiRDD.filter(lambda row:row!=header)

ratingRDD = body.filter(lambda s:s.split(",")[1]=='29')
#resultRDD = ratingRDD.countByValue()
resultRDD = ratingRDD.map(lambda s:(s.split(",")[2],1)).reduceByKey(lambda x,y:x+y)
r = resultRDD.collect()
print "******************RATINGS ARE",r
