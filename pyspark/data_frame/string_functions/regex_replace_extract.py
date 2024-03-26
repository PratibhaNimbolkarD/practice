from pyspark.sql import *
from pyspark.sql.functions import  *

spark = SparkSession.builder.appName('trim').getOrCreate()

name = [("Aarav", "Patel", "Udaipur city"),
        ("Aadya", "Sharma", "Bhopal city"),
        ("Advik", "Verma", "Indore city")]

schema = ["first_name", "last_name", "city"]

df_name = spark.createDataFrame(data=name, schema=schema)
df_name = df_name.withColumn("city", regexp_replace(col("city"), " ", "_"))
df = df_name.withColumn("first_name", regexp_extract(col("first_name"), "^\\s*(\\w+)\\s*$", 1))
df_name.show()
df.show()