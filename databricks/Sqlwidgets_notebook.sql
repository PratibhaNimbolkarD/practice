-- Databricks notebook source

CREATE WIDGET TEXT firstname DEFAULT 'Pratibha'


-- COMMAND ----------


CREATE WIDGET DROPDOWN gender DEFAULT 'male' CHOICES SELECT 'male'

-- COMMAND ----------

REMOVE WIDGET  firstname

-- COMMAND ----------

REMOVE WIDGET gender


-- COMMAND ----------

-- MAGIC %python
-- MAGIC data = [(1 , 'Pratibha' , 'Female'),(2 , 'Himansh' , 'Male'),(3 , 'Nitya', 'Female'),(4 , 'Nitin' , 'Male')]
-- MAGIC cols = ['id','name','gender']
-- MAGIC df = spark.createDataFrame(data,cols)
-- MAGIC display(df)
-- MAGIC

-- COMMAND ----------


