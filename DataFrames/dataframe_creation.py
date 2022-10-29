from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("Data Frame Starter").getOrCreate()

df = spark.read.csv("./inputData/StudentData.csv", header=True)
print(df.show())


#print(df.select("name", "course").show())

#print(df.select(df.name, df.age).show())

#print(df.select(col("roll"), col("name") ).show())

#print(df.select('*').show())

print(df.select(df.columns[2: 6]).show() )

print( df.select(df.columns[0:3], "marks", col("email")) )

df1 = df.select(col('name'), col('age'))
print(df1.show())
