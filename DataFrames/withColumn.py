from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder.appName("withColumn").getOrCreate()

df = spark.read.csv("./inputData/StudentData.csv", header=True)

print(df.printSchema())

#df = df.withColumn("marks", col("marks").cast("Integer"))

#print((df1.printSchema()))

print("****************** Manipulating Column *********************")

print(df.withColumn("marks", df.marks + 1).show())

print(df.withColumn("Country", lit("USA")).show())

print("************** Manipulating Multiple With Column")

df2 = df.withColumn("Country", lit("IND")).withColumn('marks', df.marks - 10)

print(df2.show())
