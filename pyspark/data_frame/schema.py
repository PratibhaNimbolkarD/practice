from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Schema").getOrCreate()

schema = StructType([StructField(name='Roll_no' , dataType= IntegerType()) ,
                     StructField(name = 'Name' , dataType=StringType())])
data = [(101 , "Rashida") , (102 , "Himani") , (103 , "Pratibha") ,(104 , "Anjali") , (105 , "Naina") , (106 , "Neha")]
df = spark.createDataFrame(data=data , schema=schema)
df.show()

