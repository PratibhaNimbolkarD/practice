from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('drop').getOrCreate()

data = [(1, "Pratibha", 20),
        (2, "Himani", 30),
         (3, "Neha", 40),
         (1, "Pratibha", 20),
         (5, " Neha", 20)]

schema = ["Id", "Name", "age"]

df = spark.createDataFrame(data=data , schema=schema)
df.distinct().show()