from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('drop').getOrCreate()

data = [(1, "Pratibha", 20),
        (2, "Himani", 30),
         (3, "Neha", 40),
         (1, "Pratibha", 20),
         (5, "Neha", 20)]

schema = ["Id", "Name", "age"]

df = spark.createDataFrame(data=data , schema=schema)
# remove duplicate using specific columns
df.dropDuplicates(["Name"]).show()
# remove duplicate using multiple columns
df.dropDuplicates(["Id","age"]).show()
# remove duplicate using all columns
df.dropDuplicates().show()