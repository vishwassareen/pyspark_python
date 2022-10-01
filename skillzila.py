import re
import findspark
from py4j.protocol import SUCCESS
findspark.init("/usr/local/spark")

from pyspark import SparkConf, SparkContext
conf= SparkConf().setAppName("pg1").setMaster("yarn")

from pyspark.sql.functions import *

sc= SparkContext(conf = conf)

# tweet= sc.textFile("/tmp/Tweet.csv")
# header = tweet.first() #extract header
# tweetdata = tweet.filter(lambda row: (row != header))   #filter out header

# tweetperuser=tweetdata.map(lambda x: (x.split(",")[1] , x.split(",")[0] ))
# tweetperuserid=tweetperuser.groupByKey()

# tweetperuserid.take(10)

# orders1= sc.textFile("/tmp/test.csv")


# pending_cnt=sc.accumulator(0)
# SUCCESS_cnt=sc.accumulator(0)

# def test(x,pending_cnt , SUCCESS_cnt):
#     iscomplete= x.split(",")[3] == "PENDING" 
#     if (iscomplete):
#         SUCCESS_cnt=SUCCESS_cnt.add(1)
#     else: 
#         pending_cnt=pending_cnt.add(1)
#     return iscomplete        


# final=orders1.filter(lambda x: test(x, pending_cnt , SUCCESS_cnt))

# print(pending_cnt.value)
# print(SUCCESS_cnt.value)





