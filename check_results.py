# Databricks notebook source
# MAGIC %run Users/**************/utils/python_functions

# COMMAND ----------

storageAccountName = "************"
storageAccountAccessKey = "*****************"
silverContainerName = "beesproject-silver-layer"
silverMountPoint = "/mnt/silver/"
goldContainerName = "beesproject-gold-layer"
goldMountPoint = "/mnt/gold/"
silverFolderPath = "/mnt/silver/anomesdia=20240623/"
goldFolderPath = "/mnt/gold/anomesdia=20240623/"

# COMMAND ----------

mount_container(storageAccountName, storageAccountAccessKey, silverContainerName, silverMountPoint)
mount_container(storageAccountName, storageAccountAccessKey, goldContainerName, goldMountPoint)

# COMMAND ----------

df = spark.read.parquet(silverFolderPath)
df.show()
df.printSchema()

# COMMAND ----------

df = spark.read.parquet(goldFolderPath)
df.show()
df.printSchema()

# COMMAND ----------

dbutils.fs.unmount(silverMountPoint)
dbutils.fs.unmount(goldMountPoint)

# COMMAND ----------


