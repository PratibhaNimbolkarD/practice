from pyspark.sql import *
from pyspark.sql.functions import upper, lower, col

spark = SparkSession.builder.appName('upper_lower').getOrCreate()

name = [("Aarav", "Patel", "Udaipur"),
        ("Aadya", "Sharma", "Bhopal"),
        ("Advik", "Verma", "Indore")]

schema = ["first_name" , "last_name" , "city"]

name_df = spark.createDataFrame(data=name , schema=schema)

df = name_df.withColumn("first_name", upper(col("first_name")))
df = name_df.withColumn("city" , lower(col("city")))
df.show()


