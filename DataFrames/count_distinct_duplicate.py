from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Count_distinct_duplicate").getOrCreate()

df = spark.read.csv("./inputData/StudentData.csv", header= True, inferSchema=True)

# count of DB course
print(df.filter(df.course == "DB").count())

#distinct applies on rows
#unique rows with gender and age
print("gender Age", df.select("gender", "age").distinct().count())

print(df.drop_duplicates(["gender"]).show())

print(df.drop_duplicates(["gender", "course"]).show())

