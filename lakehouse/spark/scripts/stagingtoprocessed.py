#
# Use PySpark to clean up the Apple Watch data in the staging directory and save it
# to the `processed` folder on s3 -- or in a Redshift table.
#
# The processing logic involves removing all rows with null values including rows
# in the `activity` column

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    FloatType,
    IntegerType,
    StringType,
    TimestampType
)

BUCKET_NAME = 's3://apple-watch-activity-data'
SOURCE_DIR = 'staging'
DEST_PATH = 'processed'
DB_NAME = 'watchdata'
TABLE_NAME = 'activitydata'

if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("staging-to-processed")\
        .getOrCreate()

    # read raw data from s3
    watch_data = spark.read\
        .option("inferSchema", 'true')\
        .csv(f"{BUCKET_NAME}/{SOURCE_DIR}", header=True)

    logging.info("No of samples before clean up ", watch_data.count())

    # drop null values
    watch_data_processed = watch_data.dropna(how="any")

    logging.info("No of samples after clean up ", watch_data_processed.count())

    # write data to s3
    watch_data_processed.write\
        .mode("overwrite")\
        .parquet(f"{BUCKET_NAME}/{DEST_PATH}")

    # Create Glue Tables
    watch_data_processed.registerTempTable(TABLE_NAME)

    spark.sql(f"CREATE DATABASE IF NOT EXIST {DB_NAME}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME}
        USING PARQUET
        LOCATION '{DEST_PATH}'
        AS SELECT * FROM {TABLE_NAME}
    """)
