from pyspark.sql import *
from pyspark.sql.functions import  *

spark = SparkSession.builder.appName('trim').getOrCreate()

name = [("Aarav", "Patel", "Udaipur"),
        ("Aadya", "Sharma", "Bhopal"),
        ("Advik", "Verma", "Indore")]

schema = ["first_name", "last_name", "city"]

df_name = spark.createDataFrame(data=name, schema=schema)

df_repeat = df_name.withColumn("last_name", repeat(col("last_name"), 2))
df_rpad = df_name.withColumn("first_name", rpad(col("first_name"), 10, "0"))
df_lpad = df_name.withColumn("city", lpad(col("city"), 10, "*"))
print("using repeat function")
df_repeat.show()
print("using rpad function")
df_rpad.show()
print("using lpad function")
df_lpad.show()