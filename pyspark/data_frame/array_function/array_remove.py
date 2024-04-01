from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import array_remove
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Array Functions").getOrCreate()

data = [("Amitabh", ["Physics", "Chemistry", "Maths"]),
        ("Priyanka", ["Physics", "Maths"]),
        ("Aishwarya", ["Chemistry", "Biology", "Physics"]),
        ("Shahrukh", ["Accounts", "Biology", "Chemisty"]),
        ("Deepika", ["Chemistry", "Physics"])]

schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Subject", ArrayType(StringType()), True)
])

df = spark.createDataFrame(data, schema)
result_df = df.withColumn("subjects_without_physics", array_remove("Subject", "Physics"))
result_df.show(truncate=False)