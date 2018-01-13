from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark import SparkContext,SparkConf
import math

def clean(body):

	print"I am In Clean function"
	cleanedRdd = body.map(lambda s:s.split(",")).filter(lambda s:len(s[0]) == 4).filter(lambda s:(int(s[1])) < 12).filter(lambda s: 1 <= (int(s[2])) <= 31 ).filter(lambda s: 1 <= (int(s[3])) <= 7).filter(lambda s: s[4].isdigit() and (int(s[4])) > 0).filter(lambda s: s[5].isdigit() and (int(s[5])) > 0).filter(lambda s: s[6].isdigit() and (int(s[6])) > 0).filter(lambda s: s[7].isdigit() and (int(s[7])) > 0).filter(lambda s:(len(s[8]))==2 and s[8]!="NA").filter(lambda s: (int(s[9])) > 0).filter(lambda s: s[10] != '0').filter(lambda s: s[11].isdigit and (int(s[11])) > 0).filter(lambda s: s[12].isdigit and (int(s[12])) > 0)
	#for i in l.collect():
		#print i
	#print l.count()
	return cleanedRdd

def validate(cleanedRdd):
	print"I am validate function"
	







if __name__=="__main__":
	appName = "Dataclean"
	conf = SparkConf().setAppName(appName)
	sc = SparkContext(conf = conf)
	lines = sc.textFile("file:///root/flight_analysis/small")
	header = lines.first()
	body = lines.filter(lambda Row: Row!=header)
	cleanedRdd = clean(body)
	validate(cleanedRdd)
