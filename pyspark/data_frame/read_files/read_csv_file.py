from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType , DateType
spark = SparkSession.builder.appName('Pratibha').getOrCreate()
schema=StructType([
    StructField("Index", IntegerType()),
    StructField("Custromer_id",StringType()),
    StructField("First_name", StringType()),
    StructField("Last_name", StringType()),
    StructField("Company" , StringType()),
    StructField("City", StringType()),
    StructField("Country", StringType()),
    StructField("Phone no" , StringType()),
    StructField("Phone no 2" , StringType()),
    StructField("Email" , StringType() ),
    StructField("Subscription Date", DateType()),
    StructField("Website" , StringType())
])


# using inferschema
infer_schema_df = spark.read.csv(path=r'C:\Users\PratibhaNimbolkar\Desktop\pyspark_practice\pyspark\resource\customers.csv' , header=True , inferSchema=True)
infer_schema_df.show()
infer_schema_df.printSchema()


#using custom schema reading a csv file

custom_schema_df = spark.read.csv(path=r'C:\Users\PratibhaNimbolkar\Desktop\pyspark_practice\pyspark\resource\customers.csv' , schema=schema , header=True)
custom_schema_df.show()
custom_schema_df.printSchema()