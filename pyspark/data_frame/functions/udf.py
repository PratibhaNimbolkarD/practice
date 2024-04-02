from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType,StructField,StringType,IntegerType

spark = SparkSession.builder.appName('udf').getOrCreate()
data = [("John", 25), ("Alice", 30), ("Bob", 35)]
schema = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType())
])
df = spark.createDataFrame(data, schema)
def uppercase(s):
    if s is not None:
        return s.upper()
    else:
        return None
uppercase_udf = udf(uppercase, StringType())
udf_df = df.withColumn("upper_name", uppercase_udf(df["name"]))
udf_df.show()