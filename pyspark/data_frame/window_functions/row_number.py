from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from  pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Window').getOrCreate()

emp_data = [(1,'manish',50000,'IT','m'),
(2,'vikash',60000,'sales','m'),
(3,'raushan',70000,'marketing','m'),
(4,'mukesh',80000,'IT','m'),
(5,'priti',90000,'sales','f'),
(6,'nikita',45000,'marketing','f'),
(7,'ragini',55000,'marketing','f'),
(8,'rashi',100000,'IT','f'),
(9,'aditya',65000,'IT','m'),
(10,'rahul',50000,'marketing','m'),
(11,'rakhi',50000,'IT','f'),
(12,'akhilesh',90000,'sales','m')]

schema = ["emp_id" , "emp_name" , "salary" , "dept" , "gender"]

emp_data_df = spark.createDataFrame(data=emp_data , schema= schema)

window = Window.partitionBy("dept").orderBy("salary")
using_row_no = emp_data_df.withColumn("row_no" , row_number().over(window))
using_row_no.show()