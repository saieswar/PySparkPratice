from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

host = "http://127.0.0.1"
port = "8080"
spark = SparkSession.builder.appName("WordCount Streaming").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 1)


lines = spark.readStream.format("socket")\
    .option('host', host)\
    .option('port', port)\
    .load()

lines.printSchema()
words = lines.select(explode(split(lines.value, ' ')).alias('word'))
words.printSchema()

word_counts = words.groupBy('word').count()
word_counts.printSchema()


completeOutPutQuery = word_counts\
    .writeStream\
    .outputMode('complete')\
    .format('console')\
    .start()

completeOutPutQuery.awaitTermination()

