from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark SQL").getOrCreate()

df = spark.read.csv("./inputData/StudentData.csv", inferSchema=True, header= True)

df.createOrReplaceTempView("students_table")

df1 = spark.sql("select course, gender, sum(marks) from students_table group by course, gender")

print(df1.show())

print(df.rdd.getNumPartitions())

df1.write.options(header=True).csv("./output/students")
overwrite, #appernf, #ignore, #error

print(spark.read.csv("./output/students/").show())