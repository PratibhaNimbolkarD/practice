from pyspark.sql import SparkSession
from pyspark.sql.functions import col,filter
from pyspark.sql.types import StructType, StructField, IntegerType , StringType

spark = SparkSession.builder.appName('and operation').getOrCreate()

Data = [(1, "Pratibha", 30), (2, "Himani", 25), (3, "Neha", 35), (4, "Anjali" , 36) , (5, "Naina", 26), (6, "Rashida" ,21)]
schema = StructType([
    StructField("ID" , IntegerType() , nullable=True),
    StructField("Name", StringType(), nullable=True),
    StructField("Age", StringType(), nullable=True)
])
dataframe = spark.createDataFrame(data=Data , schema=schema)
#using and operation filtered the table
using_and = dataframe.filter((col("Age")>25) & (col("ID")>=3))
using_and.show()