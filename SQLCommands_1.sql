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

-- COMMAND ----------

-- show create table customer

-- COMMAND ----------

create table customer location "dbfs:/customer_data/data"


CREATE TABLE spark_catalog.default.customer_data ( ID INT, NAME STRING, LOCATION STRING, ADDRESS STRING, ZIPCODE STRING) USING delta LOCATION 'dbfs:/customer_data/data' TBLPROPERTIES ( 'delta.minReaderVersion' = '1', 'delta.minWriterVersion' = '2')

-- COMMAND ----------

show create table customer_data

-- COMMAND ----------

drop table customer_data

-- COMMAND ----------

--show databases
--show tables
drop table customer_info

-- CREATE  TABLE customer_info
-- (
--  ID INT,
--  NAME STRING,
--  LOCATION STRING,
--  ADDRESS STRING,
--  ZIPCODE STRING
-- ) 
-- using csv location "/customer_external/csv"

-- COMMAND ----------

show create table customer_info

-- COMMAND ----------

describe database default

-- COMMAND ----------


 INSERT INTO customer_info VALUES (1,'SAIRAM','HYD','HYD','500090') 
 --show create table customer_data



-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.head("dbfs:/customer_external/csv/part-00000-tid-1280033024408666794-18390a44-d26f-487e-9281-1e5a55b1b578-126-1-c000.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("dbfs:/user/hive/warehouse"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("dbfs:/customer_data/data/_delta_log/")) 
-- MAGIC

-- COMMAND ----------

-- describe database default
describe database salesdata

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### view and temporary view and Global temparary view

-- COMMAND ----------

CREATE TABLE customer_data
(
 ID INT,
 NAME STRING,
 LOCATION STRING,
 ADDRESS STRING,
 ZIPCODE STRING
);

INSERT INTO customer_data VALUES (2,'SAIRAM','HYD','HYD','500090');

create or replace view view_customer_data as
select id,name,location,address,zipcode from customer_data;

-- show create table view_customer_data

-- CREATE VIEW default.view_customer_data ( id, name, location, address, zipcode) TBLPROPERTIES ( 'transient_lastDdlTime' = '1722763447') AS select id,name,location,address,zipcode from customer_info

create or replace temporary view temp_view_customer_data as
select id,name,location,address,zipcode from customer_data;

create or replace global temporary view gv_customer_data as
select id,name,location,address,zipcode from customer_data;

select * from view_customer_data;
select * from temp_view_customer_data;
select * from global_temp.gv_customer_data;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %fs ls dbfs:/user/hive/warehouse/productcategorycsv/
-- MAGIC
-- MAGIC %fs rm -r dbfs:/user/hive/warehouse/productcategorycsv/
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS ProductCategory;

CREATE TABLE ProductCategory
(
	ProductCategoryID int, 
	Name string
);

INSERT INTO ProductCategory (ProductCategoryID,Name)VALUES (1, 'Bikes');
INSERT INTO ProductCategory (ProductCategoryID,Name)VALUES (2, 'Components');
INSERT INTO ProductCategory (ProductCategoryID,Name)VALUES (3, 'Clothing');
INSERT INTO ProductCategory (ProductCategoryID,Name)VALUES (4, 'Accessories');

SELECT * FROM ProductCategory

DELETE FROM ProductCategory WHERE ProductCategoryID=1

DESCRIBE HISTORY ProductCategory

RESTORE TABLE ProductCategory TO VERSION AS OF 4

TRUNCATE TABLE ProductCategory

-- COMMAND ----------

CREATE TABLE ProductCategory_bkp AS SELECT * FROM ProductCategory 

SELECT * FROM ProductCategory_bkp

-- COMMAND ----------

DROP TABLE IF EXISTS ProductCategorycsv;

CREATE TABLE ProductCategorycsv
(
	ProductCategoryID int, 
	Name string
) USING CSV PARTITIONED BY (YEAR INT);

INSERT INTO ProductCategoryCSV (ProductCategoryID,Name,YEAR)VALUES (1, 'Bikes',2014);
INSERT INTO ProductCategoryCSV (ProductCategoryID,Name,YEAR)VALUES (2, 'Components',2014);
INSERT INTO ProductCategoryCSV (ProductCategoryID,Name,YEAR)VALUES (3, 'Clothing',2015);
INSERT INTO ProductCategoryCSV (ProductCategoryID,Name,YEAR)VALUES (4, 'Accessories',2015);

select * from ProductCategoryCSV;

--DELETE FROM ProductCategoryCSV  

TRUNCATE TABLE ProductCategoryCSV PARTITION (YEAR=2014)

DESCRIBE HISTORY ProductCategoryCSV

-- COMMAND ----------


