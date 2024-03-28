from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set


spark = SparkSession.builder.appName('collect list').getOrCreate()
data = [
    ("Pune", 25000),
    ("Bengaluru", 30000),
    ("Pune", 65000),
    ("Delhi", 75000),
    ("Bengaluru", 45000),
    ("Bhopal", 10000)
]
df = spark.createDataFrame(data, ["city", "population"])
collect_set = df.groupby("city").agg(collect_set("population").alias("city_set"))
collect_set.show()