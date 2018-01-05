from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *

def basic_df_example(spark):
	df = spark.read.json("file:///root/people.json")
	df.show()
	df.printSchema()
	df2 = df.select("name")
	df2.show()
	df3 = df.select(df['name'],df['age'] + 1)
	df3.show()
	df.filter(df['age']>21).show()
	df.groupBy("name").count().show()
	df.createOrReplaceTempView("people")	
	sqlDf = spark.sql("select * from people")
	sqlDf.show()
	df.createGlobalTempView("people2")
	spark.sql("select * from global_temp.people2").show()



if __name__ == "__main__":
	spark = SparkSession.builder.appName("BAsic Pyspark SQl practice").getOrCreate()
	basic_df_example(spark)
