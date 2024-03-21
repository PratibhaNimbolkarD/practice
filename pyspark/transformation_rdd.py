from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Pratibha').getOrCreate()

Data = [11 , 12 , 13 , 14 , 15 , 16 , 17 , 18 , 19 , 20]

rdd = spark.sparkContext.parallelize(Data)


result_rdd = rdd.map(lambda x: x * 2)
print("using map function " ,result_rdd.collect())

result_rdd1 = rdd.flatMap(lambda x: [x, x * 2])
print("using flat map " , result_rdd1.collect())


result_rdd2 = rdd.filter(lambda x: x % 2 == 0)
print("using filter ", result_rdd2.collect())

Data1 = ([11 , 12 , 13 , 14 , 15 , 16 , 17 , 18 , 19 , 20])

rdd1 = spark.sparkContext.parallelize(Data1)

result_rdd3 = rdd1.mapPartitions(lambda iterator: [sum(iterator)])
print("using map_partitions ", result_rdd3.collect())

result_rdd4 = rdd1.mapPartitionsWithIndex(lambda idx, iterator: [(idx, sum(iterator))])
print("using map_partitions with index ",result_rdd4.collect())