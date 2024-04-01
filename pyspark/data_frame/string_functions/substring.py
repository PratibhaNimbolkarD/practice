from pyspark.sql import *
from pyspark.sql.functions import  *

spark = SparkSession.builder.appName('trim').getOrCreate()

name = [("Aarav", "Patel", "Udaipur"),
        ("Aadya", "Sharma", "Bhopal"),
        ("Advik", "Verma", "Indore")]

schema = ["first_name", "last_name", "city"]

df_name = spark.createDataFrame(data=name, schema=schema)
df_substring = df_name.withColumn("last_name", substring(col("last_name"), 1,4))
print("using substring function")
df_substring.show()