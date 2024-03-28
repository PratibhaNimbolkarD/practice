from pyspark.sql import *


spark = SparkSession.builder.appName('Pratibha').getOrCreate()
data = [(1 , "Pratibha") , (2 , "Himani") , (3 , "Basheer") , (4 , "Krishna") , (5 , "Manvi")]
df = spark.createDataFrame(data= data , schema=['id' , 'name'])
df.show()
df.printSchema()