from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,ArrayType

spark = SparkSession.builder.appName('sample json for practice').getOrCreate()

schema = StructType([
    StructField("countries",ArrayType(
        StructType([
            StructField("name",StringType(), nullable=True),
            StructField("code", StringType(),nullable=True),
            StructField("rank" , IntegerType(), nullable=True)
        ])
    ),nullable=True)
])

json = spark.read.json('../../resource/sample json for practice.json' , multiLine=True , schema=schema)
json.show()
