from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json
from pyspark.sql.types import StructType, StringType

# Create a SparkSession
spark = SparkSession.builder.appName("JSON Conversion Example").getOrCreate()

# Sample DataFrame with JSON strings
data = [("John", '{"age": 30, "city": "New York"}'),
        ("Alice", '{"age": 25, "city": "Los Angeles"}')]
schema = ["name", "json_data"]
df = spark.createDataFrame(data, schema)

# Define the schema for JSON parsing
json_schema = StructType() \
    .add("age", StringType()) \
    .add("city", StringType())

# Parse JSON strings into structured data
parsed_df = df.withColumn("parsed_data", from_json("json_data", json_schema))

# Serialize structured data back to JSON strings
serialized_df = parsed_df.withColumn("json_string", to_json("parsed_data"))

# Show the results
parsed_df.show(truncate=False)
serialized_df.show(truncate=False)