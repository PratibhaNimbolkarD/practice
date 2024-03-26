from pyspark.sql import *
from pyspark.sql.functions import  *

spark = SparkSession.builder.appName('trim').getOrCreate()

name = [("Aarav", "Patel", "Udaipur"),
        ("Aadya", "Sharma", "Bhopal"),
        ("Advik", "Verma", "Indore")]

schema = ["first_name", "last_name", "city"]

df_name = spark.createDataFrame(data=name, schema=schema)

df = df_name.withColumn("last_name", repeat(col("last_name"), 2))
df = df.withColumn("first_name", rpad(col("first_name"), 5, "0"))
df = df.withColumn("city", lpad(col("city"), 10, "*"))
df.show()