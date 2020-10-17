
import findspark
findspark.init("/usr/local/spark") 

from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("yarn").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("/tmp/hive/test/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
