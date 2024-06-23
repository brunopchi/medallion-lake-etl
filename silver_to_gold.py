# Databricks notebook source
import pytz
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# COMMAND ----------

# MAGIC %run Users/**************/utils/python_functions

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

partitionColumn1 = "brewery_type"
partitionColumn2 = "postal_code"
storageAccountName = "************"
storageAccountAccessKey = "******************"
silverContainerName = "beesproject-silver-layer"
silverMountPoint = "/mnt/silver/"
goldContainerName = "beesproject-gold-layer"
goldMountPoint = "/mnt/gold/"

# COMMAND ----------

timezoneLocal = "America/Sao_Paulo"
timezone = pytz.timezone(timezoneLocal)
localTime = datetime.now(timezone)
formatedDate = localTime.strftime("%Y%m%d")

# COMMAND ----------

mount_container(storageAccountName, storageAccountAccessKey, silverContainerName, silverMountPoint)
mount_container(storageAccountName, storageAccountAccessKey, goldContainerName, goldMountPoint)

# COMMAND ----------

inputFolders = dbutils.fs.ls(silverMountPoint)
outputPath = goldMountPoint

for folder in inputFolders:

    if formatedDate == extract_folder_date(f"{folder.name}"):

        folderPath = f"{silverMountPoint}{folder.name}"
        outputPath = f"{goldMountPoint}{folder.name}"

        print(f"Processing {folderPath} ...")

        df = spark.read.parquet(folderPath)
        df_agg = df.groupBy(partitionColumn1, partitionColumn2) \
                    .agg(F.count("*").alias("quantity_of_breweries"))

        df_agg = df_agg.withColumn("quantity_of_breweries", F.col("quantity_of_breweries").cast(IntegerType()))

        df_agg.write.mode("overwrite").partitionBy(partitionColumn1, partitionColumn2).parquet(outputPath)
        
        print(f"Done! Parquet file saved in {outputPath}! \n")

# COMMAND ----------

dbutils.fs.unmount(silverMountPoint)
dbutils.fs.unmount(goldMountPoint)

# COMMAND ----------


