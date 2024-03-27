from pyspark.sql import *


spark = SparkSession.builder.appName('Pratibha').getOrCreate()

data1 = [(1 , "Pratibha" , 2000, 2), (2 , "Himani" , 5000 , 1) , (3 , "Neha" , 6000 , 4) ]
schema1 = ['id' ,'name' , 'salary' , 'dep']
data2 = [(1 , 'IT') , (2 , 'HR') , (3 , 'Payroll') , (4 , 'account')]
schema2 = ['id' , 'name']
empdf = spark.createDataFrame(data=data1 , schema=schema1)
depdf = spark.createDataFrame(data=data2 ,schema=schema2)

empdf.show()
depdf.show()

empdf.join(depdf , empdf.dep == depdf.id , 'right').show()
