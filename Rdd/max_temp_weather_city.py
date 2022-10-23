from pyspark import SparkContext

sc = SparkContext(master="local", appName="MaxTempCity")

weather_rdd = sc.textFile("./inputData/weather.csv")
zips_rdd = sc.textFile("./inputData/zips_city.csv")

def get_zip_codes_max_temp(lines):
    _, zip_code, temp = lines.split(",")
    return (int(zip_code), int(temp))


zip_temp_rdd = weather_rdd.map(get_zip_codes_max_temp)
max_temp_per_zip_rdd = zip_temp_rdd.reduceByKey(lambda x,y : max(x,y))

zips_city_rdd = zips_rdd.map(lambda line: (int(line.split(",")[0]), line.split(",")[1])  )
max_temp_city_rdd = max_temp_per_zip_rdd.join(zips_city_rdd)
print(max_temp_city_rdd.map(lambda x : (x[1][1], x[1][0])).collect())
