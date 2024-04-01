from pyspark.sql import SparkSession
spark  = SparkSession.builder.appName("creation").getOrCreate()
df_with_parquet = spark.read.parquet("part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet")
df_with_parquet.show()