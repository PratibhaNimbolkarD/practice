from pyspark.sql import *;
from pyspark.sql.functions import col , when

spark = SparkSession.builder.appName('when_otherwise').getOrCreate()

list = [("Pratibha", 24),
        ("Himani", 36),
        ("Kuldeep", 23),
        ("Krishna",18),
        ("Neha", 17),
        ("Anjali", 15)
        ]

schema = ["Name" , "age"]

list_df = spark.createDataFrame(data=list , schema=schema)
list_df1= list_df.withColumn("category", when(col("age")<18 , "Child").otherwise("adult"))
list_df.show()
list_df1.show()
