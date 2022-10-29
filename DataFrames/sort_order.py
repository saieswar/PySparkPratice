from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SortOrder").master("local").getOrCreate()

df = spark.read.csv("./inputData/OfficeData.csv", inferSchema=True, header=True)
print(df.show())

print(df.sort(df.bonus).show())

print(df.sort(df.age.desc(), df.salary.asc()).show())

print(df.orderBy(df.age.desc(), df.bonus.desc(), df.salary.asc() ).show())