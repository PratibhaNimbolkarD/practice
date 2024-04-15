-- Databricks notebook source
CREATE TABLE IF NOT EXISTS employee_demo (
  emp_id INT,
  emp_Name STRING,
  gender STRING,
  salary INT,
  dept STRING
) USING DELTA

-- COMMAND ----------

INSERT INTO employee_demo VALUES (100 , "Stephen", "M" , 2000 , "IT");
INSERT INTO employee_demo VALUES (200 , "Philipp", "M" , 8000 , "HR");
INSERT INTO employee_demo VALUES (300 , "LARA", "F" , 6000 , "SALES");

-- COMMAND ----------

SELECT * from employee_demo



-- COMMAND ----------

-- MAGIC %python
-- MAGIC from delta.tables import DeltaTable
-- MAGIC
-- MAGIC deltaInstance1 = DeltaTable.forPath(spark, "dbfs:/FileStore/tables/delta")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(deltaInstance1.toDF())

-- COMMAND ----------

-- MAGIC %python
-- MAGIC deltaInstance2 = DeltaTable.forName(spark , "employee_demo")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(deltaInstance2.toDF())

-- COMMAND ----------

DESCRIBE HISTORY employee_demo

-- COMMAND ----------


