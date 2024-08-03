# Databricks notebook source
display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-datasets/")) 

# COMMAND ----------

# MAGIC %fs rm -r "dbfs:/sourcefiles"

# COMMAND ----------

dbutils.fs.rm("dbfs:/sourcefiles/backup")

# COMMAND ----------

dbutils.fs.rm("dbfs:/sourcefiles/backup/Categorydata.csv")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/sourcefiles/backup/"))

# COMMAND ----------

dbutils.fs.mv("dbfs:/sourcefiles/Categorydata.csv","dbfs:/sourcefiles/backup/Categorydata.csv")

# COMMAND ----------

display(dbutils.fs.head("dbfs:/sourcefiles/Categorydata.csv"))

# COMMAND ----------

dbutils.fs.put("dbfs:/sourcefiles/Categorydata.csv","""Category,Year,Amount
Bikes,2018,200
Bikes,2019,6000
Bikes,2020,7000
Bikes,2021,8000
Components,2018,15000
Components,2019,21000
Components,2020,45000
Components,2021,19000""")

# COMMAND ----------

display(dbutils.fs.head("dbfs:/sourcefiles/adult.data"))

# COMMAND ----------

display(dbutils.fs.ls("/sourcefiles"))

# COMMAND ----------

dbutils.fs.cp("dbfs:/databricks-datasets/adult/adult.data","/sourcefiles")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-datasets/adult/"))

# COMMAND ----------

dbutils.fs.mkdirs("/sourcefiles")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/hive/warehouse/adventurework.db/customer"))

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/adventurework.db/customer

# COMMAND ----------

# MAGIC %sql
# MAGIC describe database Adventurework

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into Adventurework.customer values (1,"sairam")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table Adventurework.customer (id int ,name string)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database Adventurework 

# COMMAND ----------

dbutils.fs.ls("/dbfs/")

# COMMAND ----------

dbutils.fs.help("ls")
