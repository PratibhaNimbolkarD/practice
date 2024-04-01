from pyspark.sql import *
from pyspark.sql.functions import  *

spark = SparkSession.builder.appName('trim').getOrCreate()

name = [("Aarav", "Patel", "Uda_ipur"),
        ("Aadya", "Sharma", "Bho_pal"),
        ("Advik", "Verma", "Ind_ore")]

schema = ["first_name", "last_name", "city"]

df_name = spark.createDataFrame(data=name, schema=schema)
df_split = df_name.withColumn("split", split(col("city"), "_"))
print("using split function")
df_split.show()