from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Cache_Persist").getOrCreate()

df = spark.read.csv("./inputData/StudentData.csv", inferSchema=True, header= True)

df1 = df.groupBy("course", "gender", "age").count()
df1.cache()

df2 = df.withColumn("dummy", df.age * 100)



df2.show()

df2.show()
