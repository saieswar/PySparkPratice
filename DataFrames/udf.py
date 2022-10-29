from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, DoubleType
spark = SparkSession.builder.appName("UDF Demo").getOrCreate()

df = spark.read.csv("./inputData/OfficeData.csv", inferSchema=True, header= True)

def get_total_sal(sal,bonus):
    return sal + bonus

total_sal_udf = udf(lambda x,y : get_total_sal(x,y), IntegerType()) #registering get_total_sal as UDF

print(df.withColumn("total_sal",total_sal_udf(df.salary, df.bonus)).show())


def get_increment(sal, bonus, state):
    print("sal", sal)
    print("bonus", bonus)
    print("state", state)
    if state == "NY":
        return (sal * 10)/100 + (bonus * 5)/100
    else:
        return (sal * 12)/100 + (bonus * 3)/100

increment_udf = udf(lambda x,y,z : get_increment(x,y,z), DoubleType())

print(df.withColumn("increment", increment_udf(df.salary, df.bonus, df.state)).show())