from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split


spark = SparkSession.builder\
    .appName("ETL Demo")\
    .config("spark.jars", "E:\Spark\PySpark\postgresql-42.5.0.jar")\
    .getOrCreate()

#extraction
df = spark.read.text("./inputData/WordData.txt")
df.cache()

#transformation
df1 = df.withColumn("splittedData", split("value", " "))
df2 = df1.withColumn("words", explode("splittedData"))
words_DF = df2.select("words").groupBy("words").count()

#Load
driver ="org.postgresql.Driver"
url = "jdbc:postgresql://database-1.csnl2hd7himeew32.ap-northeast-1.rds43.amazonaws.com/"
table = "pyspark_aws.word_count"
user = 'ENV["user"]'
password = 'ENV['pwd']''

words_DF.write.format("jdbc").option("driver", driver).option("url", url).option("dbtable", table).option("mode", "append")\
    .option("user", user).option("password", password).save()