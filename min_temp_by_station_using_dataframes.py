import findspark
findspark.init("/usr/local/spark")

from pyspark.sql import SparkSession , functions as func
from pyspark.sql.types import StructType , StructField , StringType , IntegerType , FloatType

spark= SparkSession.builder.appName("min_temprature").getOrCreate()

#to give the explicit datatype for a file in which there is no datacolumn mentioned using structtype function
schema= StructType([ \
                     StructField("stationId" , StringType() , True),\
                     StructField("date", IntegerType() , True), \
                     StructField("measure_type", StringType() , True), \
                     StructField("temprature", FloatType(), True)])

df_input = spark.read.schema(schema).csv("/tmp/1800.csv")

mintemprature= df_input.filter(df_input.measure_type == "TMIN")

record= mintemprature.select("stationId","temprature").groupBy("stationId").min("temprature")

#we can create another new column on a dataframe using withcolumn
mintempratureF= record.withColumn("temprature_in_F", \
                                    func.round(func.col("min(temprature)") *0.1 * (9.0 / 5.0) + 32.0 , 2  )) \
                                        .select("temprature_in_F"  , "stationId").sort("temprature_in_F")
mintempratureF.show()
result= mintempratureF.collect()

for i in result: print(i[0],i[1])

spark.stop()


