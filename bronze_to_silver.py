# Databricks notebook source
import json
import pytz
from datetime import datetime

# COMMAND ----------

# MAGIC %run Users/**************/utils/python_functions

# COMMAND ----------

partitionColumn = "postal_code"
storageAccountName = "*************"
storageAccountAccessKey = "******************"
bronzeContainerName = "beesproject-bronze-layer"
bronzeMountPoint = "/mnt/bronze/"
silverContainerName = "beesproject-silver-layer"
silverMountPoint = "/mnt/silver/"

# COMMAND ----------

timezoneLocal = "America/Sao_Paulo"
timezone = pytz.timezone(timezoneLocal)
localTime = datetime.now(timezone)
formatedDate = localTime.strftime("%Y%m%d")

# COMMAND ----------

mount_container(storageAccountName, storageAccountAccessKey, bronzeContainerName, bronzeMountPoint)
mount_container(storageAccountName, storageAccountAccessKey, silverContainerName, silverMountPoint)

# COMMAND ----------

inputFolders = dbutils.fs.ls(bronzeMountPoint)
outputPath = silverMountPoint

for folder in inputFolders:

    if formatedDate == extract_folder_date(f"{folder.name}"):

        folderPath = f"{bronzeMountPoint}{folder.name}"
        files = dbutils.fs.ls(folderPath)
        
        for file in files:

            filePath = f"{bronzeMountPoint}{folder.name}{file.name}"
            outputPath = f"{silverMountPoint}{folder.name}"

            print(f"Processing {filePath} ...")

            df = spark.read.json(filePath)
            df = drop_null_columns(df)
            df.write.mode("overwrite").partitionBy(partitionColumn).parquet(outputPath)
            
            print(f"Done! Parquet file saved in {outputPath}! \n")

# COMMAND ----------

dbutils.fs.unmount(bronzeMountPoint)
dbutils.fs.unmount(silverMountPoint)

# COMMAND ----------


