# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE EMPLOYEE_ADD_CONSTRINT
# MAGIC (
# MAGIC   ID INT,
# MAGIC   AGE INT
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE EMPLOYEE_ADD_CONSTRINT ADD CONSTRAINT AGE_CHECK CHECK (AGE>35);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO EMPLOYEE_ADD_CONSTRINT VALUES (1,36)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE SOURCE_TABLE
# MAGIC (
# MAGIC   EMPID INT,
# MAGIC   NAME VARCHAR(100),
# MAGIC   SALARY DECIMAL(10,4),
# MAGIC   AGE INT,
# MAGIC   JOININGDATE DATE
# MAGIC );
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE TABLE TARGET_TABLE
# MAGIC (
# MAGIC   EMPID INT,
# MAGIC   NAME VARCHAR(100),
# MAGIC   SALARY DECIMAL(10,4),
# MAGIC   AGE INT,
# MAGIC   JOININGDATE DATE
# MAGIC );
# MAGIC
# MAGIC INSERT INTO SOURCE_TABLE VALUES (2,'SAI',100,35,'2020-01-01')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM source_table;
# MAGIC SELECT * FROM TARGET_TABLE;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO TARGET_TABLE AS T
# MAGIC USING SOURCE_TABLE AS S
# MAGIC ON T.EMPID = S.EMPID
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE DEFAULT;
# MAGIC
# MAGIC describe table TARGET_TABLE;
# MAGIC describe detail source_table;
# MAGIC -- describe extended source_table;
# MAGIC -- describe history source_table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe database default;
# MAGIC describe database extended default;
# MAGIC describe schema default;
# MAGIC describe schema extended default;

# COMMAND ----------

# DBTITLE 1,create a table by using CTAS COMMAND
# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS SOURCE_TABLE_DEMO
# MAGIC COMMENT 'This is a demo table'
# MAGIC -- LOCATION 'dbfs:/mnt/input/source_table_demo'
# MAGIC PARTITIONED BY (productcategory)
# MAGIC AS
# MAGIC select *, CASE WHEN _c1 =1 THEN 'BIKES'
# MAGIC                WHEN _c1 =2 THEN 'CLOTHINGS'
# MAGIC                WHEN _c1 =3 THEN 'COMPONENTS'
# MAGIC                ELSE 'OTHERS' END as productcategory
# MAGIC                 from csv.`/mnt/input/CategoryData.csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL SOURCE_TABLE_DEMO

# COMMAND ----------

dbutils.fs.ls('abfss://unity-catalog-storage@dbstorageacozdjvhxylno.dfs.core.windows.net/1421027813912874/__unitystorage/catalogs/208d8782-e822-40e1-bfa5-d7fb97651d9b/tables/4a54d914-4485-40b5-8ace-b7190d040d59')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table target_table_clone
# MAGIC deep clone target_table

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table target_table_clone1
# MAGIC shallow clone target_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from target_table_clone1

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail source_table

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO SOURCE_TABLE VALUES (2,'SAI',100,35,'2020-01-01')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history  SOURCE_TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source_table version as of 2

# COMMAND ----------

# MAGIC %sql
# MAGIC restore table source_table to version as of 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC optimize source_table
# MAGIC zorder by (empid)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum source_table retain 0 hours dry run

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum source_table retain 0 hours
