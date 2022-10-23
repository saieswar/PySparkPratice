from pyspark import SparkContext

sc = SparkContext(master='local', appName="aggregations")

pair_rdd = sc.parallelize([(1,2), (3,4), (3,6), (3,6), (1,3)], 2)

print("Partitions for pair_rdd : ", pair_rdd.getNumPartitions())
print("Map Values : ", pair_rdd.mapValues(lambda x: x*2).collect()) #multiplying values
print("FlatMap Values : ", pair_rdd.flatMapValues(lambda x : range(0, x)).collect()) #explode
print("keys :", pair_rdd.keys().collect())
print("Values : ", pair_rdd.values().collect())
print(("SortByKey : ", pair_rdd.sortByKey().collect()))
print("Reduce By key:", pair_rdd.reduceByKey(lambda x,y : x + y).collect())
print("Group By Key")
for k,v in  pair_rdd.groupByKey().collect():
    print(k,list(v))

first_rdd = sc.parallelize([(1,2), (3,4), (3,6), (3,6), (1,3)])
second_rdd = sc.parallelize([(3,9), (2,3)])

print("co group")
for k,v in  first_rdd.cogroup(second_rdd).collect():
    print(k,list(v))

