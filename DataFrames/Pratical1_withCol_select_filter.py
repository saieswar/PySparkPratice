# 1) Read Data from student data as DF
# 2) Create a new col with 120 marks as val
# 3) Create new col with avg marks of each student
# 4) Filter out all those who achieved more than 80% in OOP course and save in new DF
# 5) Filter out all those who achieved more than 80% in Cloud course and save in new DF
# 6) Print Names and marks of all students from above DF's


from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

spark = SparkSession.builder.appName("Demo1").getOrCreate()

#Read Data from student data as DF
student_df = spark.read.csv("./inputData/StudentData.csv", header= True, inferSchema=True)

#Create a new col with 120 marks as val
df1 = student_df.withColumn("total_marks", lit(120))
print(df1.show())

#Create new col with avg marks of each student
df2 = df1.withColumn("avg_marks", (df1.marks/df1.total_marks) * 100)
print(df2.show())

#Filter out all those who achieved more than 80% in OOP course and save in new DF
marks_greater_than_80_in_oops = df2.filter( (df2.course == "OOP") & (df2.avg_marks > 80) )

#Filter out all those who achieved more than 80% in Cloud course and save in new DF
marks_greater_than_60_in_cloud = df2.filter( (df2.course == "Cloud") & (df2.avg_marks > 60) )

#Print Names and marks of all students from above DF's
print(marks_greater_than_80_in_oops.select("name", "marks").show())
print(marks_greater_than_60_in_cloud.select("name", "marks").show())