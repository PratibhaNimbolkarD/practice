from pyspark.sql import SparkSession
from pyspark.sql.functions import mean

spark = SparkSession.builder.appName('mean').getOrCreate()


data = [
    ("Pune", 25000),
    ("Bengaluru", 30000),
    ("Pune", 65000),
    ("Delhi", 75000),
    ("Bengaluru", 45000),
    ("Bhopal", 10000)
]
df = spark.createDataFrame(data, ["city", "population"])

agg_df = df.groupby("city").agg(mean("population").alias("mean_of_population"))

agg_df.show()
