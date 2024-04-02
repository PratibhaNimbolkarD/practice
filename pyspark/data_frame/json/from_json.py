from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType
spark = SparkSession.builder \
    .appName("JSON Conversion Example") \
    .getOrCreate()
data = [("John", '{"age": 30, "city": "New York"}'),
        ("Alice", '{"age": 25, "city": "Los Angeles"}')]
schema = ["name", "json_data"]
df = spark.createDataFrame(data, schema)
json_schema = StructType().add("age", StringType()).add("city", StringType())

#from_json
parsed_df = df.withColumn("parsed_data", from_json("json_data", json_schema))
parsed_df.show(truncate=False)