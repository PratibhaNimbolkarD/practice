from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce

spark = SparkSession.builder.appName("Handling Missing Values").getOrCreate()

data = [("John", 30, None),
        ("Alice", None, 45000),
        ("Bob", 35, 60000),
        ("Emily", None, None)]

columns = ["Name", "Age", "Salary"]
df = spark.createDataFrame(data, columns)

df_filtered = df.filter(df.Name.isNotNull() & df.Age.isNotNull() & df.Salary.isNotNull())
print("DataFrame after filtering nulls:")
df_filtered.show()

df_replaced = df.fillna({"Name": "Unknown", "Age": 0, "Salary": -1})
print("DataFrame after replacing nulls:")
df_replaced.show()

df_coalesced = df.withColumn("Non_Null_Value", coalesce(df.Age, df.Salary))
print("DataFrame after using coalesce:")
df_coalesced.show()

df_no_nulls = df.na.drop()
print("DataFrame after dropping rows with nulls:")
df_no_nulls.show()

