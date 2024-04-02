from pyspark.sql.functions import upper
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

spark = SparkSession.builder.appName('transform').getOrCreate()
data = [(1, "Pratibha", 2000),
        (2, "Kuldeep", 3000),
        (3, "Krishna", 4000),
        (4, "Manvi", 1500)
        ]

schema = StructType([
    StructField("ID" , IntegerType()),
    StructField("Name", StringType()),
    StructField("Salary", IntegerType())
])

df = spark.createDataFrame(data=data , schema=schema)
def coverttoUpper(df):
    return df.withColumn("Name", upper(df.Name))

def doubleTheSalary(df):
    return df.withColumn("Salary", df.Salary*2)


df1 = df.transform(coverttoUpper).transform(doubleTheSalary)
df1.show()
