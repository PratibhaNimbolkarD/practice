from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = SparkSession.builder.appName('count').getOrCreate()

data = [
    ("Pune", 25000),
    ("Bengaluru", 30000),
    ("Pune", 65000),
    ("Delhi", 75000),
    ("Bengaluru", 45000),
    ("Bhopal", 10000)
]

df = spark.createDataFrame(data, ["city", "population"])
count = df.groupby("city").agg(count("*").alias("row_count"))
count.show()