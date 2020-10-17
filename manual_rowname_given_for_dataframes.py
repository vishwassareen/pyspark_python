import findspark
findspark.init("/usr/local/spark")

from pyspark.sql import SparkSession
from pyspark.sql import Row

spark= SparkSession.builder.appName("sparksql").getOrCreate()

def mapper(line):
    fields= line.split(",")
    return Row(ID=  int(fields[0]), name = str(fields[1]), age=int(fields[2]), numfriends= int(fields[3]))



lines= spark.sparkContext.textFile("/tmp/fakefriends.csv")
people= lines.map(mapper)

schemapeople= spark.createDataFrame(people).cache()
schemapeople.createOrReplaceTempView("people")

teenager= spark.sql("select * from people where age >=13 and age <=19")

schemapeople.groupBy("age").count().orderBy("age").show()

for i in teenager.collect(): print(i)

spark.stop()