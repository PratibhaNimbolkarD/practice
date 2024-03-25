from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder .appName("AliasExample").getOrCreate()

data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["Name", "Age"])
df_alias = df.select(col("Name").alias("Full_Name"))

df_alias.show()