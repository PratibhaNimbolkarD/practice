from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import expr

spark = SparkSession.builder.appName('pratibha').getOrCreate()

data = [('IT',8,5),
        ('Payroll',3,2),
        ('HR',2,4)]

schema = StructType([
    StructField("dept" , StringType()),
    StructField("male", StringType()),
    StructField("female", StringType())
])

df = spark.createDataFrame(data=data , schema=schema)
unpivot_df = df.select('dept' , expr("Stack(2,'M', male , 'F' , female) as (gender , count)"))
unpivot_df.show()
