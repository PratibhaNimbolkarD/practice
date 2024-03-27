from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sort merge").getOrCreate()

data1 = [("A", 1), ("B", 2), ("C", 3)]
columns1 = ["key", "value"]
df1 = spark.createDataFrame(data=data1, schema=columns1)

data2 = [("A", "X"), ("B", "Y"), ("C", "Z")]
columns2 = ["key", "another_value"]
df2 = spark.createDataFrame(data=data2, schema=columns2)

df1_sorted = df1.sort("key")
df2_sorted = df2.sort("key")

joined_df = df1_sorted.join(df2_sorted, "key")

joined_df.show()

spark.stop()
