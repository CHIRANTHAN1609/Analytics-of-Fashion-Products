# Databricks notebook source
# MAGIC %md 
# MAGIC #Analytics of Fashion products using PySpark RDD and DataFrames

# COMMAND ----------

from pyspark import SparkContext 
sc = SparkContext.getOrCreate() 
dataframe = sc.textFile("/FileStore/tables/myntra_products_catalog.csv")
dataframe.collect()

# COMMAND ----------

def Func(lines):
  lines = lines.upper() 
  lines = lines.split() 
  return lines 
dataframe1 = dataframe.map(Func) 
dataframe1.take(5)

# COMMAND ----------

dataframe2 = dataframe.flatMap(Func) 
dataframe2.take(5)

# COMMAND ----------

dataframe.count()

# COMMAND ----------

dataframe_mapped = dataframe.map(lambda x: (x,1)) 
dataframe_grouped = dataframe_mapped.groupByKey()

# COMMAND ----------

print(list((j[0], list(j[1])) for j in dataframe_grouped.take(5)))

# COMMAND ----------

dataframe_mapped.reduceByKey(lambda x,y: x+y).map(lambda x:(x[1],x[0])).sortByKey(False).take(10)

# COMMAND ----------

sample1 = dataframe_mapped.sample(False,.2,42) 
sample2 = dataframe_mapped.sample(False,.2,42) 
join_on_sample1_sample2 = sample1.join(sample2) 
join_on_sample1_sample2.take(2)

# COMMAND ----------

dataframe.getNumPartitions()

# COMMAND ----------

dataframe_coalesce = dataframe.coalesce(1) 
dataframe_coalesce.getNumPartitions()

# COMMAND ----------

num_dataframe = sc.parallelize(range(1,1000)) 
num_dataframe.reduce(lambda x,y: x+y)

# COMMAND ----------

num_dataframe.max(),num_dataframe.min(), num_dataframe.sum(),num_dataframe.variance(),num_dataframe.stdev()

# COMMAND ----------


