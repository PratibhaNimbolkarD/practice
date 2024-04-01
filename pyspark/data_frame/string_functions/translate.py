from pyspark.sql import *
from pyspark.sql.functions import  col , translate

spark = SparkSession.builder.appName('trim').getOrCreate()

name = [("Aarav", "Patel", "Udaipur"),
        ("Aadya", "Sharma", "Bhopal"),
        ("Advik", "Verma", "Indore")]

schema = ["first_name", "last_name", "city"]

df_name = spark.createDataFrame(data=name, schema=schema)
df_translate = df_name.withColumn("city" , translate(col("city"), "aeiou" , "12345"))
print("using translate")
df_translate.show()