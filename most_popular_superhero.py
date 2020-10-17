import findspark
findspark.init()

from pyspark.sql import functions as func 
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType , StructField , StructType , IntegerType , FloatType

spark= SparkSession.builder.appName("famous_movie_hero").getOrCreate()


schema= StructType([ \
                    StructField("id" , IntegerType(), True), \
                    StructField("name", StringType(), True)  ])

names= spark.read.schema(schema).option("sep" , " ").csv("/tmp/Marvel_Names.txt")      

lines= spark.read.text("/tmp/Marvel_Graph.txt")

linescsv= lines.withColumn("id", func.split(func.col("value"), " ")[0]) \
          .withColumn("connection_count", func.size(func.split(func.col("value"), " ")) -1) \
          .groupBy("id").agg(func.sum("connection_count").alias("connection_count"))

mostpopular = linescsv.sort(func.col("connection_count").desc()).first()

mostpopularname= names.filter(func.col("id") == mostpopular[0]).select("name").first()

for i in mostpopularname: 
    print( mostpopularname[0] + ": " + str(mostpopular[1]))