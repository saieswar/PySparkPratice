from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Grouping Demo").getOrCreate()

df = spark.read.csv("./inputData/StudentData.csv", inferSchema=True, header= True)

#Always Apply Aggregation if we are using GroupBY
print(df.groupBy("gender").sum("marks").show())

print(df.groupBy("gender").count().show())
print(df.groupBy("course").count().show())

print(df.groupBy("gender").max("marks").show())
print(df.groupBy("gender").min("marks").show())

print(df.groupBy("age").avg("marks").show())