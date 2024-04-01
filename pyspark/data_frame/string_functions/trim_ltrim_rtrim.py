from pyspark.sql import *
from pyspark.sql.functions import trim , ltrim , rtrim , col , translate

spark = SparkSession.builder.appName('trim').getOrCreate()

name = [("   Aarav ", "Patel   ", "   Udaipur   "),
        ("   Aadya  ", "Sharma  ", "    Bhopal   "),
        ("   Advik  ", "Verma  ", "     Indore   ")]

schema = ["first_name", "last_name", "city"]

df_name = spark.createDataFrame(data=name, schema=schema)
df_ltrim_rtrim_trim = df_name.withColumn("last_name", trim(col("last_name")))
df_ltrim_rtrim_trim = df_ltrim_rtrim_trim.withColumn("city", rtrim(col("city")))
df_ltrim_rtrim_trim= df_ltrim_rtrim_trim.withColumn("first_name" , ltrim(col("first_name")))


df_ltrim_rtrim_trim.show()