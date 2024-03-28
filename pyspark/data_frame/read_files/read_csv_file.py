from pyspark.sql import *

spark = SparkSession.builder.appName('Pratibha').getOrCreate()



df = spark.read.csv(path=r'C:\Users\PratibhaNimbolkar\Downloads\customers-100.csv' , header=True , inferSchema=True)
df.show()
df.printSchema()


