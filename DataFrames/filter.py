from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Filter Demo").master("local").getOrCreate()

df = spark.read.csv("./inputData/StudentData.csv", header=True)

df1 = df.filter( (df.course == "DB") & (df.marks > 50) )

courses = ["Cloud", "OOP", "DSA"]

print(df.where( df.course.isin(courses) ).show())

print(df.filter(df.course.startswith("D")).show())

print( df.where(df.name.contains("se")).show())

print(df.filter(df.name.like('%se%')))