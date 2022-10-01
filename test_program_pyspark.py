import findspark
findspark.init("/usr/local/spark") 

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("accumulator variable program")
sc = SparkContext(conf = conf)

order=sc.textFile("/tmp/orders.csv")

a=sc.accumulator(0) 
b=sc.accumulator(0)

def Product_status(orders, x,y):
    iscompleted=((orders.split(",")[3] == "COMPLETED") or (orders.split(",")[3] == "CLOSED"))
    if (iscompleted == True):
        a= x.add(1)
    else:
        b=y.add(1)
    return iscompleted

orderfilter= order.filter(lambda k: Product_status(k , a ,b))
result=orderfilter.count()
print(a.value)
print(b.value)
