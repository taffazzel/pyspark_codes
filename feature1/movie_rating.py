from pyspark import SparkContext, SparkConf
import collections


def rating():
	conf = SparkConf().setMaster("local").setAppName("MovieRating")
	sc = SparkContext(conf = conf)
	fiRDD = sc.textFile("file:///home/hdfs/ml-20m/smallrating.csv")
	header = fiRDD.first()
	body = fiRDD.filter(lambda row:row!=header)
	print "Number of lines in the file %s",body.count()
	ratingRDD = body.map(lambda s:s.split(",")[2])
	resultRDD = ratingRDD.countByValue()
	resultSortedRDD = collections.OrderedDict(sorted(resultRDD.items()))
	for k,v in resultSortedRDD.items():
		print "%s %i"%(k,v)


if __name__="__main__":
	rating()


