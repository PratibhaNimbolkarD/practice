from pyspark.sql import SparkSession

spark = SparkSession.builder .appName("Shuffle Hash ") .getOrCreate()


data1 = [("A", 1), ("B", 2), ("C", 3)]
columns1 = ["key", "value"]
df1 = spark.createDataFrame(data=data1, schema=columns1)


data2 = [("A", "X"), ("B", "Y"), ("D", "Z")]
columns2 = ["key", "another_value"]
df2 = spark.createDataFrame(data=data2, schema=columns2)

joined_df = df1.join(df2, "key")


joined_df.show()