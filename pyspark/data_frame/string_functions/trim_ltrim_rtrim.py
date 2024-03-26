from pyspark.sql import *
from pyspark.sql.functions import trim , ltrim , rtrim , col , translate

spark = SparkSession.builder.appName('trim').getOrCreate()

name = [("   Aarav ", "Patel   ", "   Udaipur   "),
        ("   Aadya  ", "Sharma  ", "    Bhopal   "),
        ("   Advik  ", "Verma  ", "     Indore   ")]

schema = ["first_name", "last_name", "city"]

df_name = spark.createDataFrame(data=name, schema=schema)
df = df_name.withColumn("last_name", trim(col("last_name")))
df = df.withColumn("city", rtrim(col("city")))
df = df.withColumn("first_name" , ltrim(col("first_name")))


df.show()