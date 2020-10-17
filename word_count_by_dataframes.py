import findspark
findspark.init("/usr/local/spark")

from pyspark.sql import SparkSession , functions as func

spak= SparkSession.builder.appName("word_count").getOrCreate()

InputDF = spak.read.text("/tmp/book.txt")

#explode work same as flatmap function
words= InputDF.select(func.explode(func.split(InputDF.value, '\\W+')).alias("words"))  
word= words.filter(words.words != "")

lowercaseword= word.select(func.lower(word.words).alias("lower_words"))

wordcount= lowercaseword.groupBy("lower_words").count()

result_sorted= wordcount.sort("count")

result= result_sorted.show(result_sorted.count())


