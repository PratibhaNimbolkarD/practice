from pyspark.sql import SparkSession
from pyspark.sql.functions import asc , desc

spark = SparkSession.builder.appName("sort").getOrCreate()
data = [("Alice", 35), ("Bob", 20), ("Charlie", 36)]
df = spark.createDataFrame(data, ["Name", "Age"])

df_asc = df.orderBy(asc("Age"))
df_desc = df.orderBy(desc("Age"))

df_asc.show()
df_desc.show()