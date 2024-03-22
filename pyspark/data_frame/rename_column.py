from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Rename').getOrCreate()

data = [("Pratibha" , 23) , ("Himani" , 35) , ("Ajeet" , 34) , ("Neha" , 28)]

df = spark.createDataFrame(data=data , schema=['Name' , 'Age'])
df = df.withColumnRenamed("Age" , "full_age")
df.show()