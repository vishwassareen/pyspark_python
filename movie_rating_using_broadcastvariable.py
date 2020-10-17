import findspark
findspark.init("/usr/local/spark")

from pyspark.sql import functions as func 
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType , StructField , StructType , IntegerType , FloatType

import codecs

spark= SparkSession.builder.appName("movies_rating").getOrCreate()


def loadmoviesname():
    movienames= {}
    with codecs.open("/home/test/Downloads/ml-100k/u.item" , "r" , encoding='ISO-8859-1' ,errors='ignore') as f:
        for lines in f:
            field=lines.split('|')
            movienames[int(field[0])]= field[1]
    return movienames        

nameDict= spark.sparkContext.broadcast(loadmoviesname())



schema= StructType([ \
                      StructField("user_id", IntegerType() , True), \
                      StructField("movie_id" , IntegerType() , True ) , \
                      StructField("rating" , IntegerType(), True) , \
                      StructField("Timestamp", StringType() , True)])


df_input= spark.read.option("sep", "\t").schema(schema).csv("/tmp/hive/test/u.data")

record_specific= df_input.select("movie_id", "rating")

result= record_specific.groupBy("movie_id").count()

def lookupName(movie_id):
    return nameDict.value[movie_id]

lookupNameudf= func.udf(lookupName)

moviewithnames= result.withColumn("movie_Name", lookupNameudf(func.col("movie_id")))

sortresult= moviewithnames.orderBy(func.desc("count"))

sortresult.show(5)

spark.stop()