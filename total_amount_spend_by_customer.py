import findspark
findspark.init("/usr/local/spark")

from pyspark import SparkContext , SparkConf
conf= SparkConf().setAppName("total_amount_spend_by_customer").setMaster("local")
sc=SparkContext( conf = conf)

lines= sc.textFile("/tmp/customer-orders.csv")
custdata= lines.map(lambda x: (x.split(",")[0], float(x.split(",")[2])))
totalamount=custdata.reduceByKey(lambda x,y: x+y)

result = totalamount.map(lambda x: (x[1], x[0])).sortByKey().collect()

for key , value in result: print(round(key,2) , value)



