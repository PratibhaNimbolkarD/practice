from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import col


spark = SparkSession.builder.appName('column add').getOrCreate()
schema = StructType([StructField(name= 'Name' , dataType=  StringType()),
                     StructField(name= 'Maths' , dataType=IntegerType()),
                     StructField(name='Physics' , dataType=IntegerType()),
                     StructField(name='Chemistry' , dataType=IntegerType())])
data = [("Pratibha " , 95 , 66 , 88) , ("Himani" , 88 , 78 , 98) , ("Neha" , 87 , 96 , 67) , ("Anjali" , 89 , 87 , 58)]
df = spark.createDataFrame(data=data , schema=schema)
df = df.withColumn("Total" , col("Maths")+col("Physics")+col("Chemistry"))
df = df.withColumn("Average" , (col("Maths")+col("Physics")+col("Chemistry"))/3)
df.show()
