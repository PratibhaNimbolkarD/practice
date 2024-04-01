from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType , LongType

spark = SparkSession.builder.appName('Pratibha').getOrCreate()
people_schema = StructType([
    StructField("people", ArrayType(StructType([
        StructField("firstName", StringType(), nullable=True),
        StructField("lastName", StringType(), nullable=True),
        StructField("gender", StringType(), nullable=True),
        StructField("age", IntegerType(), nullable=True),
        StructField("number", StringType(), nullable=True)
    ])), nullable=True)
        ])
df_json_schema = spark.read.json("sample4.json" , schema=people_schema , multiLine=True)
# reading the json with custom schema
print("Reading the json with custom schema")
df_json_schema.show(truncate=False)
df_json_schema.printSchema()

