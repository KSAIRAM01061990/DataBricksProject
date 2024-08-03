-- Databricks notebook source
show databases

-- COMMAND ----------

show tables

-- COMMAND ----------

CREATE DATABASE SPARK_SQL

-- COMMAND ----------

DESCRIBE DATABASE SPARK_SQL

-- COMMAND ----------

CREATE TABLE SPARK_SQL.CUSTOMER(ID INT , NAME VARCHAR(100))


-- COMMAND ----------

INSERT INTO SPARK_SQL.CUSTOMER VALUES(1,'SAIRAM')

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/spark_sql.db

-- COMMAND ----------

describe table SPARK_SQL.CUSTOMER


-- COMMAND ----------

show databases
