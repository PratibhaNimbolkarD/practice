from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("UnionVsUnionAll").getOrCreate()

data1 = [("Alice", 25), ("Bob", 30)]
data2 = [("Charlie", 35), ("Alice", 25), ("David", 40),("David", 40)]

df1 = spark.createDataFrame(data1, ["Name", "Age"])
df2 = spark.createDataFrame(data2, ["Name", "Age"])

union_df = df1.union(df2)
print("Result of union:")
union_df.show()


print("union_all")
union_all_df = df1.unionAll(df2)
union_all_df.show()


