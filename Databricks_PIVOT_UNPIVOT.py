# Databricks notebook source
data = [ 
        ("Banana", 1000, "USA"), 
        ("Carrots", 1500, "USA"),
        ("Beans", 1600, "USA"), 
        ("Orange", 2000, "USA"),
        ("Orange", 2000, "USA"), 
        ("Banana", 400, "China"),
        ("Carrots", 1200, "China"), 
        ("Beans", 1500, "China"),
        ("Orange", 4000, "China"), 
        ("Banana", 2000, "Canada"),
        ("Carrots", 2000, "Canada"), 
        ("Beans", 2000, "Mexico")
        ]
column = ["Product", "Amount", "Country"]
df = spark.createDataFrame(data=data,schema=column)
display(df)

# COMMAND ----------

df_pivot=df.groupBy("product").pivot("country").sum("amount")
display(df_pivot)

# COMMAND ----------

df_unpivot=df_pivot.selectExpr("Country", "stack(4, 'Banana', Banana, 'Beans', Beans, 'Carrots', Carrots, 'Orange', Orange) as (Country,Total)")
display(df_unpivot)
