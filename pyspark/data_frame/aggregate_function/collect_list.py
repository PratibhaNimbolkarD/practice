from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list


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

collect_list = df.groupby("city").agg(collect_list("population").alias("city_list"))
collect_list.show()