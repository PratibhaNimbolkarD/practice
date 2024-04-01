
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from pyspark.sql.functions import *

spark = SparkSession.builder.appName('lead').getOrCreate()
simpleData = [("James", "Sales", 3000),
              ("Michael", "Sales", 4600),
              ("Robert", "Sales", 4100),
              ("James", "Sales", 3000),
              ("Saif", "Sales", 4100),
              ("Maria", "Finance",3000),
              ("Scott", "Finance",3300),
              ("Jen", "Finance",3900),
              ("Kumar","Marketing",2000)]

schema = ["employee_name" , "department","salary"]
df = spark.createDataFrame(data=simpleData , schema=schema)
window = Window.partitionBy("department").orderBy("salary")
using_lag = df.withColumn("lag", lag("salary" ,1).over(window))
using_lag.show()
