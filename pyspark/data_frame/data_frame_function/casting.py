from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

data = [("John", 25), ("Jane", 30), ("Bob", 35)]

spark = SparkSession.builder.appName("Casting").getOrCreate()
df = spark.createDataFrame(data, ["Name", "Age"])
df = df.withColumn("Age", col("Age").cast(IntegerType()))
df.show()
df.printSchema()