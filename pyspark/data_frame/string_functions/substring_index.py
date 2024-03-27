

from pyspark.sql import *
from pyspark.sql.functions import  col , substring_index

spark = SparkSession.builder.appName('trim').getOrCreate()

name = [("Aarav", "Patel", "Udaipur"),
        ("Aadya", "Sharma", "Bhopal"),
        ("Advik", "Verma", "Indore")]

schema = ["first_name", "last_name", "city"]

df_name = spark.createDataFrame(data=name, schema=schema)
df_name = df_name.withColumn("substring1", substring_index(col("first_name"), "a", 2))
#if we use -1 then it will print substring after a
df_name = df_name.withColumn("substring2", substring_index(col("first_name"),"a", -1))
df_name.show()