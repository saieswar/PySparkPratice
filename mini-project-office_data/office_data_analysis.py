from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,avg,max,min,mean,count,udf
from pyspark.sql.types import DoubleType,IntegerType
spark = SparkSession.builder.appName("office_data_analysis").master("local").getOrCreate()

df1 = spark.read.csv("./data/OfficeDataProject.csv", header=True, inferSchema=True)
df1.cache()
#Total Number of Employees in a company
print(df1.select("*").count())

#Total Number of Departments in a company
print(df1.select("department").distinct().count())

#Department names of the company
print(df1.select("department").distinct().show())

#total number of employees in each department
print(df1.groupBy("department").count().show())

#total number of employees in each state
print(df1.groupBy("state").count().show())

#total number of employees in each state and each dept
print(df1.groupBy("state", "department").count().show())

#min and max salaries in each dept and sort salaries in asc
df2 = df1.groupBy("department").agg(min("salary").alias("min"), max("salary").alias("max"))
print(df2.orderBy(df2.min.asc(), df2.max.asc()).show())

#emplotess working in NY state under Finance Dept whose bonus > avg bonus of employees in NY state
df2 = df1.filter( (df1.state == "NY")).groupBy("state").agg(avg(df1.bonus).alias("avg_bonus"))
avg_bonus = df2.select("avg_bonus").collect()[0]["avg_bonus"]
print(df1.filter((df1.state == "NY") & (df1.department == "Finance") & (df1.bonus > avg_bonus)).show())

#Raise the Salaries $500 od all emp whose age is greater than 45
def sal_inc_age_grt_45(age,sal):
    if age > 45:
        return sal + 500
    return sal

sal_inc_udf = udf(lambda x,y : sal_inc_age_grt_45(x,y), IntegerType())

print(df1.withColumn("salary", sal_inc_udf(df1.age,df1.salary) ).show())

df1.filter(df1.age > 45).write.csv("./output/office")