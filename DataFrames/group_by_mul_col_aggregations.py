from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,avg,max,min,count
spark = SparkSession.builder.appName("Grouping Mul Col").getOrCreate()
#
# df = spark.read.csv("./inputData/StudentData.csv", inferSchema=True, header= True)
#
# print(df.groupBy("course", "gender").count().show())
#
# print(df.groupBy("course", "gender").sum("marks").show())
#
# print(df.groupBy("course", "gender").agg(count('*'), sum("marks"), min("marks"), max("marks"), avg("marks") ).show())

ofc_df = spark.read.csv("./inputData/OfficeData.csv", inferSchema=True, header=True)

print(ofc_df.groupBy("department").agg(sum("salary"), min("salary")).show())

print(ofc_df.groupBy("department", "state").agg(sum("salary"), min("salary")).show())

