from pyspark import SparkContext

sc = SparkContext(master="local", appName='weather')

inputRdd = sc.textFile("E:\Spark\weather.csv", 3)

year_temp_rdd = inputRdd.map(lambda line: (int(line.split(',')[0].split('-')[0]), int(line.split(',')[2])))
max_temp_year = year_temp_rdd.reduceByKey(lambda x, y: max(x, y))
sortedrdd = max_temp_year.sortByKey()
sortedrdd.saveAsTextFile("./outputData")