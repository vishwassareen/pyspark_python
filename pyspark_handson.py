
import imp
import re
from shlex import quote
from typing import cast
import findspark
findspark.init("/usr/local/spark")

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.functions import col, date_format,sum,avg,max , min

from pyspark.sql.types import StringType , StructField , StructType

from pyspark.sql.window import Window as w, WindowSpec
import sys

spark= SparkSession.builder.appName("program1").getOrCreate()

# ## reading the data frame data from here 

# employee =spark.read.format("csv") \
#     .options(inferschema='true' , sep=',' , header='true' ) \
#         .load("/tmp/dvdrental/employee.csv")
        
# customer =spark.read.format("csv") \
#     .options(inferschema='true' , sep=',' , header='true' ) \
#         .load("/tmp/dvdrental/customer.csv")


# payment =spark.read.format("csv") \
#     .options(inferschema='true' , sep=',' , header='true' ) \
#         .load("/tmp/dvdrental/payment.csv")

# rental =spark.read.format("csv") \
#     .options(inferschema='true' , sep=',' , header='true' ) \
#         .load("/tmp/dvdrental/rental.csv")

# film =spark.read.format("csv") \
#     .options(inferschema='true' , sep=',' , header='true' ) \
#         .load("/tmp/dvdrental/film.csv")

# inventory =spark.read.format("csv") \
#     .options(inferschema='true' , sep=',' , header='true' ) \
#         .load("/tmp/dvdrental/inventory.csv")

# orders1 =spark.read.format("csv") \
#     .options( sep=',' , header='true' , inferSchema='true' ) \
#         .load("/tmp/test.csv").toDF("order_id", "date", "amount", "status")


# dataset1 =spark.read.format("csv") \
#     .options( sep=',' , header='true' , inferSchema='true' ) \
#         .load("/tmp/dataset1.csv")


# dataset2 =spark.read.format("csv") \
#     .options( sep=',' , header='true' , inferSchema='true' ) \
#         .load("/tmp/dataset2.csv")


# from pyspark.sql.functions import datediff , to_date, current_date

# resultset= dataset1.join(dataset2, dataset1.empid== dataset2.empid , "inner").drop(dataset2.empid) \
#     .drop_duplicates() \
#         .fillna({"cn": 'NA'}) \
#             .withColumn("cur_date",to_date(current_date(),"dd/MM/yyyy")) \
#                 .withColumn("age", round(datediff(func.col("cur_date") , to_date(func.col("dob"), "dd/MM/yyyy")) /365 ,0) ) \
#                     .select( "cc", "cn", "empid", "salary", "empname", "dob","age") 


# resultset.show(10)

# WindowSpec=w.partitionBy("order_id").orderBy("order_id")

# result=orders1.withColumn("newcol" , func.lead("amount",1).over(WindowSpec)) \
#     .withColumn("date" , to_date(func.col("date"), 'yyyy-MM-dd')) \
#         .filter(func.col("newcol") != 0 ) \
#             .filter(func.col("newcol") < func.col("amount")) \

# result.show(10)



# a_data =spark.read.format("csv") \
#     .options( sep=',' , header='true' , inferSchema='true' ) \
#         .load("/tmp/a.csv").toDF("id", "name")



# b_data =spark.read.format("csv") \
#     .options( sep=',' , header='true' , inferSchema='true' ) \
#         .load("/tmp/b.csv").toDF("id", "name")


# notexistrecord= b_data.join(a_data , a_data.id == b_data.id, "left_anti")
# existrecord= b_data.join(a_data , a_data.id == b_data.id, "left_semi")

# incr_data=a_data.union(notexistrecord)

# replaceDf = incr_data.join(existrecord, incr_data.id == existrecord.id, how='left_semi')

# resultDf = incr_data.subtract(replaceDf).union(existrecord)

# resultDf.show(10)

# from pyspark.sql.types import StringType , StructField , StructType 

# struct = StructType( [
#   StructField("firstCol", StringType(), True), \
#   StructField("secondCol", StringType(),True)])

# head_data =spark.read.format("csv") \
#     .options( delimiter=',' , header='true' , inferSchema='true' , quote='\"'   ) \
#         .option("mode", "DROPMALFORMED").schema(struct) \
#             .load("/input/headerfile.csv")


# head_data.show()


a_data =spark.read.format("csv") \
    .options( sep=','  , header='True',inferSchema='true' ) \
        .load("/input/wipro_ex.csv")

# d_data=a_data.select([func.col(col).alias(col.replace(' ', '_')) for i in a_data.col1 ])

data=a_data.rdd.map(lambda x: x[0])


for i in data.collect():
    f=a_data.withColumn("newcol" , func.col("i") )

f.show()

