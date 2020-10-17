import findspark
findspark.init("/usr/local/spark") 

from pyspark import SparkConf, SparkContext
import collections

conf=SparkConf().setMaster("local").setAppName("fake_friend_by_age")
sc= SparkContext(conf=conf)

lines=sc.textFile("/tmp/fakefriends.txt")
result= lines.map(lambda x: (int(x.split(",")[2]) , int(x.split(",")[3])))
resultvalue = result.mapValues(lambda x: (x,1))
totalfriendsbyage= resultvalue.reduceByKey(lambda x,y: (x[0] + y[0] , x[1] + y[1]) )
averagefriendbyage = totalfriendsbyage.mapValues(lambda x: int(x[0] / x[1])).collect()

for i in averagefriendbyage:
    print(i)

