import findspark
findspark.init("/usr/local/spark")

from pyspark.sql import functions as func 
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType , StructField , StructType , IntegerType , FloatType

spark= SparkSession.builder.appName("movies_rating").getOrCreate()

schema= StructType([ \
                      StructField("user_id", IntegerType() , True), \
                      StructField("movie_id" , IntegerType() , True ) , \
                      StructField("rating" , IntegerType(), True) , \
                      StructField("Timestamp", StringType() , True)])

df_input= spark.read.option("sep", "\t").schema(schema).csv("/tmp/hive/test/u.data")

record_specific= df_input.select("movie_id", "rating")

result= record_specific.groupBy("movie_id").count().orderBy(func.desc("count"))


result.show(5)

spark.stop()