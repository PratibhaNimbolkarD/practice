from pyspark.sql import *

spark = SparkSession.builder.appName('left_semi').getOrCreate()

employee_data = [(10 , "Raj" , "1999" , "100" , "H" , 2000),
                 (20 , "Rahul" , "2002" , "200" , "M" , 8000),
                 (30 , "Raghav" , "2010" , "100", "" , 6000 ),
                 (40 , "Raja" , "2004" , "100", "F" , 7000),
                 (50 , "Rama" , "2008", "400" ,"F", 1000),
                 (60 , "Rasul" , "2014" , "500", "M" , 5000)
                 ]

employee_schema = ["employee_id" , "name" , "doj" , "employee_dept_id" , "gender" , "salary"]
employee_df = spark.createDataFrame(data=employee_data , schema=employee_schema)

department_data = [("HR" , 100),
                   ("Supply" , 200),
                   ("Sales" , 300),
                   ("Stock" , 400)
                   ]

department_schema = ["dept_name" , "dept_id"]
department_df = spark.createDataFrame(data=department_data , schema= department_schema)

df_join = employee_df.join(department_df , employee_df.employee_dept_id == department_df.dept_id , "semi")

employee_df.show()
department_df.show()
df_join.show()