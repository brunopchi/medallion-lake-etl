# Databricks notebook source
import re
from pyspark.sql.functions import col

# COMMAND ----------

def extract_folder_date(input_string: str):
    """
    Function to extract 8 digits (in a row) from a string.

    Args:
        input_string (str): String to be evaluated.

    Returns:
        extracted_date (str): String with date format as yyyymmdd
    """

    try:
        pattern = r'\d{8}'
        match = re.search(pattern, input_string)

        if match:
            return match.group()
        else:
            return None
    except TypeError as err:
        print(err.args)
        raise

# COMMAND ----------

def mount_container(storageAccountName: str, storageAccountAccessKey: str, containerName: str, mountPoint: str):
    """
    Function to mount containers from Azure Data Lake.

    Args:
        storageAccountName (str): Name of the storage account
        storageAccountAccessKey (str): Access Key of the storage account
        containerName (str): Name of the container or layer (bronze, silver, gold)
        mountPoint (str): Name given to the mount point
    
    Returns:
        None
    """

    if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
        try:
            dbutils.fs.mount(
                source = f"wasbs://{containerName}@{storageAccountName}.blob.core.windows.net",
                mount_point = mountPoint,
                extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
            )
            print(f"mount {mountPoint} succeeded!")
            return None
        except Exception as err:
            print(err.args)
            raise

# COMMAND ----------

def drop_null_columns(spark_df):
    """
    Function to remove columns from a Spark DataFrame that contain only null values.
    
    Args:
        spark_df (DataFrame): Input Spark DataFrame
    
    Returns:
        df (DataFrame): DataFrame with null columns removed
    """

    columns_to_drop = []

    for col_name in spark_df.columns:
        
        non_null_count = spark_df.filter(col(col_name).isNotNull()).count()
        
        if non_null_count == 0:
            columns_to_drop.append(col_name)
    
    df = spark_df.drop(*columns_to_drop)
    
    return df


# COMMAND ----------


