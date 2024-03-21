from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Pratibha').getOrCreate()

Data = [11 , 12 , 13 , 14 , 15 , 16 , 17 , 18 , 19 , 20]

rdd = spark.sparkContext.parallelize(Data)



# min() - Find the minimum element in the RDD based on default ordering
min_value = rdd.min()
print("Min value:", min_value)

# max() - Find the maximum element in the RDD based on default ordering
max_value = rdd.max()
print("Max value:", max_value)

# mean() - Calculate the arithmetic mean of elements in the RDD
mean_value = rdd.mean()
print("Mean value:", mean_value)