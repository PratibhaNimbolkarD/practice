from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Pratibha').getOrCreate()

Data = [11 , 12 , 13 , 14 , 15 , 16 , 17 , 18 , 19 , 20]

rdd = spark.sparkContext.parallelize(Data)


# reduce() - Aggregate the elements of the RDD using a function
result = rdd.reduce(lambda a, b: a + b)
print("Sum of numbers:", result)

# countByKey() - Count the occurrences of each key
result1 = rdd.countByKey()
print("Key counts:", result1)