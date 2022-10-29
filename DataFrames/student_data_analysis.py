from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,avg,min,max
spark = SparkSession.builder.appName("Grouping Demo").getOrCreate()

df = spark.read.csv("./inputData/StudentData.csv", inferSchema=True, header= True)

#total number of students enrollerd in each course
print(df.groupBy("course").count().show())

#total number of male and female students in each course
print(df.groupBy("course", "gender").count().show())

#total marks achieved by each gender in each course
print(df.groupBy("course", "gender").sum("marks").show())

#min, max, avg marks achieved in each course by each age group
print(df.groupBy("course", "age").agg(min("marks"), max("marks"), avg("marks")).show())