import findspark
findspark.init("/usr/local/spark")

from pyspark import SparkConf,SparkContext
conf= SparkConf().setMaster("yarn").setAppName("min_temp_by_station")
sc= SparkContext(conf = conf)

lines= sc.textFile("/tmp/1800.csv")
requiredfield= lines.map(lambda x: (x.split(",")[0], x.split(",")[2] , float(x.split(",")[3]) * 0.1 * (0.9 / 0.5) + 32.0 ))
requiredfield_Tmin= requiredfield.filter(lambda x: "TMIN" in x[1])
stationstemp= requiredfield_Tmin.map(lambda x: (x[0] , x[2]))
mintemp_bystation=stationstemp.reduceByKey(lambda x,y: min(x,y)).collect()

for i in mintemp_bystation: print(i[0] , i[1])
