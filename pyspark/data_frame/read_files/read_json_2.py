from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType , LongType

spark = SparkSession.builder.appName('Pratibha').getOrCreate()
people_schema = StructType([
    StructField("name", StringType(), nullable=True),
    StructField("phoneNumber", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True),
    StructField("address", StringType(), nullable=True),
    StructField("userAgent", StringType(), nullable=True),
    StructField("hexcolor", StringType(), nullable=True)
])
df_json_schema = spark.read.json(r'C:\Users\PratibhaNimbolkar\Desktop\pyspark_practice\pyspark\resource\sample 2 for practice.json' , schema=people_schema , multiLine=True)
# reading the json with custom schema
print("Reading the json with custom schema")
df_json_schema.show(truncate=False)
df_json_schema.printSchema()
