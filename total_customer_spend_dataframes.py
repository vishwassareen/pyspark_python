
from itertools import count
import findspark
findspark.init("/usr/local/spark")

from pyspark.sql import functions as func
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType , StructField , StringType , StructType , FloatType 

spark= SparkSession.builder.appName("customer_total_spend").getOrCreate()

schema = StructType([ \
                    StructField("cust_id" , IntegerType() , True), \
                    StructField("item_id", IntegerType() , True), \
                    StructField("amount" , FloatType() , True)])

df_input= spark.read.schema(schema).csv("/tmp/customer-orders.csv")

selectedresult= df_input.select("cust_id", "amount")

result = selectedresult.groupBy("cust_id").agg(func.round(func.sum("amount"),2).alias("Total_Money_spend_by_customer")).sort("Total_Money_spend_by_customer")
result.show(result.count())

spark.stop()