from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

spark = SparkSession.builder.appName('and operation').getOrCreate()

Data = [(1, "Pratibha", 30), (2, "Himani", 25), (3, "Neha", 35), (4, "Anjali" , 36) , (5, "Naina", 26), (6, "Rashida" ,21)]
schema = StructType([
    StructField("ID" , IntegerType() , nullable=True),
    StructField("Name", StringType(), nullable=True),
    StructField("Age", StringType(), nullable=True)
])

df = spark.createDataFrame(data=Data,schema=schema)
# using or operation filtered the table
using_or = df.filter((col("Name")>2) | (col("Age")<30))
using_or.show()


