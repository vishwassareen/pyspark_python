from email import header
import findspark
findspark.init("/usr/local/spark")

from pyspark import SparkContext , SparkConf 
# conf= SparkConf().setAppName("total_amount_spend_by_customer").setMaster("local")
# sc=SparkContext( conf = conf)

# lines= sc.textFile("/tmp/customer-orders.csv")
# lines= lines.map(lambda x: (x.split(",")[0], float(x.split(",")[2])))

# for i in lines.take(10):
#     print(i)
# # totalamount=custdata.reduceByKey(lambda x,y: x+y)

# # result = totalamount.map(lambda x: (x[1], x[0])).sortByKey().collect()

# # for key , value in result: print(round(key,2) , value)

from pyspark.sql import SparkSession , functions as func

spark= SparkSession.builder.appName("word_count").getOrCreate()

lines= spark.read.option("header",True).csv("/tmp/emp_dup.csv")
# df= lines.toDF("id","deptname","empname","salary")

#Convert columns to Map
from pyspark.sql.functions import col,lit,create_map
from pyspark.sql.functions import to_json

df = lines.withColumn("propertiesMap",create_map(
        lit("empname"),col("empname"),\
        lit("salary"),col("salary")
        )).\
        withColumn("dict" , to_json(col("propertiesMap"))).drop("salary","empname","propertiesMap")
df.printSchema()
df.show(truncate=False)

