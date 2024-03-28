from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, min, max, abs, round
spark = SparkSession.builder.appName('numeric function').getOrCreate()

dataframe = [(1, 3.14159), (2, -2.71828) , (3, 3.45673)]

schema = ["Id", "Value"]

df = spark.createDataFrame(data=dataframe , schema=schema)
sum_value = df.select(sum("Value")).collect()[0][0]
avg_value = df.select(avg("Value")).collect()[0][0]
min_value = df.select(min("Value")).collect()[0][0]
max_value = df.select(max("Value")).collect()[0][0]
abs_value = df.select("Id", abs("Value")).alias("abs_value")
rounded_df = df.select("Id", round("Value", 2).alias("RoundedValue"))


print("The sum of value is:", sum_value)
print("The avg of value is:", avg_value)
print("The min of value is:", min_value)
print("The max of value is:", max_value)
rounded_df.show()
abs_value.show()