from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

spark = SparkSession.builder.appName("explode").getOrCreate()

data = [
    ("Aakash", 25, ["apple", "banana", "orange"], "New York", "Male"),
    ("Himani", 30, ["grape", "pear"], "Los Angeles", "Female"),
    ("Neha", 35, ["watermelon", "kiwi", "mango"], "Chicago", "Female"),
    ("Junaid", 40, ["pineapple", "strawberry"], "Houston", "Male"),
    ("Manisha", 28, ["blueberry", "raspberry", "blackberry"], None, "Female")
]
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Fruits", ArrayType(StringType()), True),
    StructField("Location", StringType(), True),
    StructField("Gender", StringType(), True)
])
df = spark.createDataFrame(data=data , schema=schema)

exploded_df = df.select("Name", "Age", explode("Fruits").alias("Fruit"), "Location", "Gender")
exploded_df.show()