from pyspark import SparkContext

sc = SparkContext(master='local', appName="WordCount")

inputRdd = sc.textFile("E:\Spark\ebook.txt", 2)
# lines_rdd = inputRdd.flatMap(lambda line : line.split(" "))
# word_one_rdd = lines_rdd.map(lambda word: (word,1))
# print(word_one_rdd.reduceByKey(lambda x,y: x+y).collect())

result = inputRdd.flatMap(lambda line: line.split(" "))\
                 .map(lambda word: (word , 1))\
                 .reduceByKey(lambda  x,y : x+y)\
                 .collect()
print(result)

# Another Method

results = inputRdd.flatMap(lambda line: line.split(" "))\
    .countByValue()
print(results)