from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Pratibha').getOrCreate()



Data1 = ([11 , 12 , 13 , 14 , 15 , 16 , 17 , 18 , 19 , 20])

rdd1 = spark.sparkContext.parallelize(Data1)

result_rdd3 = rdd1.mapPartitions(lambda iterator: [sum(iterator)])
print("using map_partitions ", result_rdd3.collect())

result_rdd4 = rdd1.mapPartitionsWithIndex(lambda idx, iterator: [(idx, sum(iterator))])
print("using map_partitions with index ",result_rdd4.collect())