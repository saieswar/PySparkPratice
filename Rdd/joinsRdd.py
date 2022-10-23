from pyspark import SparkContext

sc = SparkContext(appName="joins", master="local")
first_rdd = sc.parallelize([(1,2), (3,4), (3,6), (3,6), (1,3)])
second_rdd = sc.parallelize([(3,9), (2,3)])

inner_join_rdd = first_rdd.join(second_rdd)

print("inner Join : ", inner_join_rdd.collect())

left_join_rdd = first_rdd.leftOuterJoin(second_rdd)
print("left join : ", left_join_rdd.collect())

right_join_rdd = first_rdd.rightOuterJoin(second_rdd)
print("Right Join : ", right_join_rdd.collect())