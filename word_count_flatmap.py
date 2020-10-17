import re
import findspark
findspark.init("/usr/local/spark")

from pyspark import SparkConf, SparkContext
conf= SparkConf().setAppName("Wordcount").setMaster("yarn")

sc= SparkContext(conf = conf)

def normalizewords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

file= sc.textFile("/tmp/book.txt")
normtext=file.flatMap(normalizewords)
words= normtext.flatMap(lambda x: x.split(" "))
wordcount= words.map(lambda x: (x,1))
takewordcount= wordcount.reduceByKey(lambda x,y: x+y).map(lambda x: (x[1],x[0]) ).sortByKey()

result=takewordcount.collect()

for key , value in result: print(key , value)

