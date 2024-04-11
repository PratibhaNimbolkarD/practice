from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,ArrayType
from pyspark.sql import functions as f
from pyspark.sql.functions import explode

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
# explode_df = json.select(explode("countries").alias("countries"))
# json.show()
# explode_df.show(truncate=False)
df_b_zip=json.withColumn("zipped_value",f.arrays_zip("countries"))
df_b_exp=df_b_zip.withColumn("explode",f.explode("zipped_value"))
# flatten
df_output=df_b_exp.withColumn("name",df_b_exp["Explode.countries.name"]).withColumn("code",df_b_exp["Explode.countries.code"]).withColumn("rank",df_b_exp["Explode.countries.rank"]).drop("countries").drop("zipped_value")

df_output.show(truncate=False)

