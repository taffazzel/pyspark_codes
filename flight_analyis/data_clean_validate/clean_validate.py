from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark import SparkContext,SparkConf
import math

def clean(body):

	print"I am In Clean function"
	l = body.map(lambda s:s.split(",")).filter(lambda s:len(s[0]) == 4).filter(lambda s:(int(s[1])) < 12).filter(lambda s: 1 <= (int(s[2])) <= 31 ).filter(lambda s: 1 <= (int(s[3])) <= 7).filter(lambda s: s[4].isdigit() and (int(s[4])) > 0)
	#l = body.map(lambda s: s.split(",")).map(lambda s:len(s[0]))
	for i in l.collect():
		print i
	#print l.count()







if __name__=="__main__":
	appName = "Dataclean"
	conf = SparkConf().setAppName(appName)
	sc = SparkContext(conf = conf)
	lines = sc.textFile("file:///root/flight_analysis/small")
	header = lines.first()
	body = lines.filter(lambda Row: Row!=header)
	clean(body)
