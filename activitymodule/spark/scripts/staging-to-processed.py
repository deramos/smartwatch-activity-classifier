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
    StringType,
    TimestampType
)

BUCKET_NAME = os.getenv('BUCKET', 's3://apple-watch-activity-data')
SOURCE_DIR = os.getenv('STAGING_PATH', 'data/staging/subset/')
DEST_PATH = os.getenv('PROCESSED_PATH', 'data/processed')
DB_NAME = os.getenv('DB_NAME', 'watchdata')
TABLE_NAME = os.getenv('TABLE_NAME', 'activitydata')


if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("staging-to-processed")\
        .getOrCreate()

    # set spark configs
    spark.conf.set('spark.sql.adaptive.enabled', True)
    spark.conf.set('spark.sql.adaptive.skewJoin.enabled', True)

    # define data schema
    schema = StructType([
        StructField("stamp", TimestampType(), nullable=True),
        StructField("yaw", FloatType(), nullable=True),
        StructField("pitch", FloatType(), nullable=True),
        StructField("roll", FloatType(), nullable=True),
        StructField("rotation_rate_x", FloatType(), nullable=True),
        StructField("rotation_rate_y", FloatType(), nullable=True),
        StructField("rotation_rate_z", FloatType(), nullable=True),
        StructField("user_acceleration_x", FloatType(), nullable=True),
        StructField("user_acceleration_y", FloatType(), nullable=True),
        StructField("user_acceleration_z", FloatType(), nullable=True),
        StructField("location_type", StringType(), nullable=True),
        StructField("latitude_distance_from_mean", FloatType(), nullable=True),
        StructField("longitude_distance_from_mean", FloatType(), nullable=True),
        StructField("altitude_distance_from_mean", FloatType(), nullable=True),
        StructField("course", FloatType(), nullable=True),
        StructField("speed", FloatType(), nullable=True),
        StructField("horizontal_accuracy", FloatType(), nullable=True),
        StructField("vertical_accuracy", FloatType(), nullable=True),
        StructField("battery_state", StringType(), nullable=True),
        StructField("user_activity_label", StringType(), nullable=True),
    ])

    # read raw data from s3
    watch_data = spark.read\
        .option("mode", "DROPMALFORMED")\
        .csv(f"{BUCKET_NAME}/{SOURCE_DIR}", header=True, schema=schema)

    # drop null values
    watch_data_processed = watch_data.filter("user_activity_label is not null")  # watch_data.dropna(how="any")

    # percentage = (watch_data_processed.count() / watch_data.count()) * 100
    # logging.info(f"Percentage of clean samples {percentage}% ")

    # write data to s3
    watch_data_processed.write\
        .mode("overwrite")\
        .parquet(f"{BUCKET_NAME}/{DEST_PATH}")

    # Create Glue Tables
    watch_data_processed.createOrReplaceTempView(TABLE_NAME)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME}
        USING PARQUET
        LOCATION '{DEST_PATH}'
        AS SELECT * FROM {TABLE_NAME}
    """)
