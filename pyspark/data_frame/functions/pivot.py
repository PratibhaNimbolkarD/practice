from pyspark.sql import SparkSession
from pyspark.sql.types import  StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import expr
spark = SparkSession.builder.appName('Pivot').getOrCreate()
data = [
        ("Pratibha", "Maths", 85),
        ("Pratibha", "Physics", 90),
        ("Shivani", "Maths", 70),
        ("Shivani", "Physics", 80),
        ("Shivani", "Chemistry", 75),
        ("Krishna", "Maths", 60 ),
        ("Krishna", "Chemistry" ,67),
        ("Krishna", "Physics" , 87)
        ]

schema = StructType([
    StructField("Name", StringType()),
    StructField("Subject", StringType()),
    StructField("Score", IntegerType())
])

df = spark.createDataFrame(data=data , schema=schema)
pivot_df =  df.groupBy("Name").pivot("Subject").agg(expr("first(Score)"))
pivot_df.show()