# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.testing import assertDataFrameEqual

# COMMAND ----------

# MAGIC %run Users/**************/utils/python_functions

# COMMAND ----------

def test_extract_folder_date(inputString: str, expectedOutput: str):
    
    assert extract_folder_date(inputString) == expectedOutput
    print("extract_folder_date - OK!")


def test_mount_container(inputString1: str, inputString2: str, inputString3: str, inputString4: str, expectedOutput):

    assert mount_container(inputString1, inputString2, inputString3, inputString4) == expectedOutput
    print("mount_container - OK!")


def test_drop_null_columns(inputDataFrame, expectedOutput):

    filteredDataframe = drop_null_columns(inputDataFrame)
    assertDataFrameEqual(filteredDataframe, expectedOutput)
    print("drop_null_columns - OK!")

# COMMAND ----------

test_extract_folder_date("Folder_20230622_SomeDescription", "20230622")
test_extract_folder_date("Folder_20230_SomeDescription", None)
test_extract_folder_date("/anomesdia=20240620/", "20240620")
test_extract_folder_date("/anomesdia=2024620", None)

# COMMAND ----------

storageAccountName = "*********"
storageAccountAccessKey = "********"
blobContainerName = "beesproject-bronze-layer"
mountPoint = "/mnt/bronze/"

test_mount_container(storageAccountName, storageAccountAccessKey, blobContainerName, mountPoint, None)

dbutils.fs.unmount(mountPoint)

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

testStructure1 = {
    'testTable1': StructType([
        StructField("country", StringType(), True),
        StructField("state", StringType(), True),
        StructField("city", StringType(), True)
    ])
}

resultStructure1 = {
    'resultTable1': StructType([
        StructField("country", StringType(), True),
        StructField("state", StringType(), True)
])}

testData1 = [("United States", None, None), (None, "Oklahoma", None), (None, None, None)]
resultData1 = [("United States", None), (None, "Oklahoma"), (None, None)]

inputDataFrame = spark.createDataFrame(testData1, testStructure1["testTable1"])
expectedoutput = spark.createDataFrame(resultData1, resultStructure1["resultTable1"])

test_drop_null_columns(inputDataFrame, expectedoutput)

# COMMAND ----------


