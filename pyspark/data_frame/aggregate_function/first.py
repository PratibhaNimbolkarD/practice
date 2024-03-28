from pyspark.sql import SparkSession
from pyspark.sql.functions import first

spark = SparkSession.builder.appName('first').getOrCreate()

data = [
    ("Pune", 25000),
    ("Bengaluru", 30000),
    ("Pune", 65000),
    ("Delhi", 75000),
    ("Bengaluru", 45000),
    ("Bhopal", 10000)
]

df = spark.createDataFrame(data, ["city", "population"])
count_distinct = df.groupby("city").agg(first("population").alias("first_city"))
count_distinct.show()