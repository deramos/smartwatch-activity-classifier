import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier

from pyspark.sql.types import StructType, StructField, FloatType, StringType, TimestampType

load_dotenv('../../.env')

BUCKET_NAME = os.getenv('BUCKET')
SOURCE_DIR = os.getenv('PROCESSED_PATH')
DB_NAME = os.getenv('DB_NAME')
TABLE_NAME = os.getenv('TABLE_NAME')
AWS_ACCESS_KEY = os.getenv('DB_NAME')
AWS_SECRET_KEY = os.getenv('TABLE_NAME')


if __name__ == '__main__':
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages: org.apache.spark:spark-hadoop-cloud_2.12:3.2.0'
    os.environ['PYSPARK_SUBMIT_ARGS'] = "--master local[*] pyspark-shell"

    # Create Spark Session
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0,org.apache.hadoop:hadoop-client:3.2.0') \
        .config('spark.jars.packages', 'org.apache.spark:spark-hadoop-cloud_2.12:3.2.0') \
        .config('spark.hadoop.fs.s3.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .config('spark.jars.excludes', 'com.google.guava:guava') \
        .appName("train-activity-model") \
        .getOrCreate()

    # Set AWS KEY
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_KEY)

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

    # load data
    data = spark \
        .read \
        .option("mode", "DROPMALFORMED") \
        .parquet(f"{BUCKET_NAME}/{SOURCE_DIR}")

    # Clean and Preprocess data
    string_cols = ['location_type', 'battery_state']
    string_indexer = StringIndexer(inputCols=string_cols, outputCols=[s + '_indexed' for s in string_cols])

    # fit and transform string data to string indexed data
    string_indexer_model = string_indexer.fit(data)
    data_indexed = string_indexer_model.transform(data)
