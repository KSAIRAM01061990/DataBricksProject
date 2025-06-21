# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION fn_getCategoryData (Categoryid int)
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC SELECT CONCAT("CategoryName : ", MAX(NAME)) FROM productcategory_csv WHERE ProductCategoryID=fn_getCategoryData.Categoryid

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *,fn_getCategoryData(ProductCategoryID) FROM productcategory_csv

# COMMAND ----------

def fn_email(emailname):
  return emailname[0]

# COMMAND ----------

display(fn_email("sairam_kanamarlapudi@epam.com"))

# COMMAND ----------

# DBTITLE 1,create and apply UDF
fn_email_udf = udf(fn_email)

# COMMAND ----------

# DBTITLE 1,register the function to use in sql query's

from pyspark.sql.types import  StringType
spark.udf.register("fn_email", fn_email)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,fn_email("sai@gmail.com") as email from productcategory_csv

# COMMAND ----------

# DBTITLE 1,Alternate we can register by using python decorators syntax
from pyspark.sql.functions import udf

@udf("string")
def fn_email(email:str) -> str:
    return email[0]

# COMMAND ----------

display(fn_email("sai@gmail.com"))

# COMMAND ----------

# DBTITLE 1,by using pandas we can register the udf
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf("string")
def fn_email_pandas(email:pd.Series) -> pd.Series:
    return email.str[0]

# COMMAND ----------

spark.udf.register("fn_email_pandas", fn_email_pandas)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,fn_email_pandas("saira@gmail.com") from productcategory_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- # CREATE TABLE EMPLOYEE_DATA
# MAGIC -- # (
# MAGIC -- #   EMPID INT,
# MAGIC -- #   NAME VARCHAR(100),
# MAGIC -- #   SALARY DECIMAL(10,4),
# MAGIC -- #   JOININGDATE DATE
# MAGIC -- # );
# MAGIC
# MAGIC ALTER TABLE EMPLOYEE_DATA 
# MAGIC SET TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled');
# MAGIC
# MAGIC ALTER TABLE EMPLOYEE_DATA 
# MAGIC ALTER COLUMN JOININGDATE SET DEFAULT current_date();
# MAGIC
# MAGIC
# MAGIC INSERT INTO EMPLOYEE_DATA (EMPID,NAME,SALARY) VALUES 
# MAGIC (1,'SAIRAM',1000),
# MAGIC (2,'KUMAR',2000)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM EMPLOYEE_DATA
# MAGIC

# COMMAND ----------


