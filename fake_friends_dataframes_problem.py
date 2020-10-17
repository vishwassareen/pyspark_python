import findspark
findspark.init("/usr/local/spark")

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql import Row

spark= SparkSession.builder.appName("sparksql").getOrCreate()

#if we have a big file consist of 200 columns and need to find the datatype for each column then use Inferschema functioj in option to get automatic datatype retrival 

lines= spark.read.option("header","true").option("inferschema", "true").csv("/tmp/fakefriends-header.csv")

friendsbyage= lines.select("age","friends")

friendsbyage.groupBy("age").agg(func.round(func.avg("friends") , 2).alias("average_friends_byage")).sort("age").show()

spark.stop()
