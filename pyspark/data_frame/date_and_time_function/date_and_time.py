
from pyspark.sql import SparkSession

from pyspark.sql.functions import current_date, current_timestamp, date_add, datediff, year, month, day, to_date, date_format

spark = SparkSession.builder.appName("DateTimeFunctions").getOrCreate()

data = [("2024-03-25",), ("2024-01-01",)]

df = spark.createDataFrame(data, ["date_str"])
df = df.withColumn("current_date", current_date())
df = df.withColumn("current_timestamp", current_timestamp())
df = df.withColumn("next_week_date", date_add(df["date_str"], 7))
df = df.withColumn("days_diff", datediff(df["next_week_date"], df["date_str"]))
df = df.withColumn("year", year(df["date_str"]))
df = df.withColumn("month", month(df["date_str"]))
df = df.withColumn("day", day(df["date_str"]))
df = df.withColumn("parsed_date", to_date(df["date_str"]))
df = df.withColumn("formatted_date", date_format(df["parsed_date"], "yyyy-MM-dd"))

df.show()
