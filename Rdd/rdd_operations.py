
from pyspark import SparkContext

sc = SparkContext(appName="rddOps", master='local')
myCollection  = "Spark unified Engine: Simple Processing sample data".split(" ")
words_rdd = sc.parallelize(myCollection, 2)
def word_starts_withs(word):
    return  word.startswith("S")

print("============ Filter Transformation ==============================")
# ============ Filter Transformation ==============================
word_start_s_rdd = words_rdd.filter(lambda word: word_starts_withs(word))
print(word_start_s_rdd.collect())
print("=================================================================")

print("======================Map Transformation ========================")

map_trans = words_rdd.map(lambda word :  (word, word[0], word_starts_withs(word)))

print(map_trans.collect())
print("=================================================================")

print("======================Sort Transformation =======================")

sort_rdd = words_rdd.sortBy(lambda word :  len(word)-1 )
print(sort_rdd.collect())
print("=================================================================")

print("=================== Reduce Action =======================")
def wordLengthReducer(left, right):
    if len(left) > len(right):
        return  left
    else:
        return right

reduce = words_rdd.reduce(wordLengthReducer)

print(reduce)

print("=================================================================")

