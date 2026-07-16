
# PySpark RDD Transformations and Actions Handbook

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDD Handbook").getOrCreate()
sc = spark.sparkContext

# ---------------------------
# CREATE RDD
# ---------------------------
rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
pair = sc.parallelize([
    ("CS",45000),
    ("IT",55000),
    ("CS",60000),
    ("ECE",40000),
    ("IT",70000)
])

# ---------------------------
# TRANSFORMATIONS
# ---------------------------
print("map")
print(rdd.map(lambda x:x*2).collect())

print("flatMap")
words=sc.parallelize(["hello world","pyspark rdd"])
print(words.flatMap(lambda x:x.split()).collect())

print("filter")
print(rdd.filter(lambda x:x%2==0).collect())

print("distinct")
print(sc.parallelize([1,1,2,2,3]).distinct().collect())

print("union")
print(rdd.union(sc.parallelize([11,12])).collect())

print("intersection")
print(rdd.intersection(sc.parallelize([5,6,7,20])).collect())

print("subtract")
print(rdd.subtract(sc.parallelize([1,2,3])).collect())

print("cartesian")
print(sc.parallelize([1,2]).cartesian(sc.parallelize(["A","B"])).collect())

print("groupBy")
print(rdd.groupBy(lambda x:x%2).mapValues(list).collect())

print("reduceByKey")
print(pair.reduceByKey(lambda a,b:a+b).collect())

print("groupByKey")
print(pair.groupByKey().mapValues(list).collect())

print("aggregateByKey")
print(pair.aggregateByKey(0, lambda a,b:a+b, lambda a,b:a+b).collect())

print("combineByKey")
cb=pair.combineByKey(
    lambda v:(v,1),
    lambda acc,v:(acc[0]+v,acc[1]+1),
    lambda a,b:(a[0]+b[0],a[1]+b[1])
)
print(cb.collect())

print("sortBy")
print(pair.sortBy(lambda x:x[1]).collect())

print("sortByKey")
print(pair.sortByKey().collect())

print("keys")
print(pair.keys().collect())

print("values")
print(pair.values().collect())

print("mapValues")
print(pair.mapValues(lambda x:x+1000).collect())

print("flatMapValues")
print(pair.flatMapValues(lambda x:[x,x+1]).collect())

pair2=sc.parallelize([("CS","A"),("IT","B"),("ME","D")])

print("join")
print(pair.join(pair2).collect())

print("leftOuterJoin")
print(pair.leftOuterJoin(pair2).collect())

print("rightOuterJoin")
print(pair.rightOuterJoin(pair2).collect())

print("fullOuterJoin")
print(pair.fullOuterJoin(pair2).collect())

print("cogroup")
print(pair.cogroup(pair2).mapValues(lambda x:(list(x[0]),list(x[1]))).collect())

print("sample")
print(rdd.sample(False,0.4,7).collect())

print("repartition")
print(rdd.repartition(4).getNumPartitions())

print("coalesce")
print(rdd.repartition(4).coalesce(2).getNumPartitions())

print("zip")
print(sc.parallelize([1,2,3]).zip(sc.parallelize(["A","B","C"])).collect())

print("zipWithIndex")
print(rdd.zipWithIndex().collect())

print("zipWithUniqueId")
print(rdd.zipWithUniqueId().collect())

print("glom")
print(rdd.glom().collect())

# ---------------------------
# ACTIONS
# ---------------------------
print("collect", rdd.collect())
print("count", rdd.count())
print("first", rdd.first())
print("take", rdd.take(3))
print("top", rdd.top(3))
print("takeOrdered", rdd.takeOrdered(3))
print("reduce", rdd.reduce(lambda a,b:a+b))
print("fold", rdd.fold(0, lambda a,b:a+b))
print("aggregate", rdd.aggregate(0, lambda a,b:a+b, lambda a,b:a+b))
print("countByValue", rdd.countByValue())
print("countByKey", pair.countByKey())
print("lookup", pair.lookup("CS"))

print("foreach")
rdd.foreach(lambda x: None)

print("foreachPartition")
rdd.foreachPartition(lambda it: [x for x in it])

print("takeSample", rdd.takeSample(False,4,10))

print("isEmpty", sc.emptyRDD().isEmpty())

# save examples (commented)
# rdd.saveAsTextFile("text_out")
# rdd.saveAsPickleFile("pickle_out")

spark.stop()
