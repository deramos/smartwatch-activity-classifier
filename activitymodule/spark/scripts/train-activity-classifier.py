import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier

from pyspark.sql.types import StructType, StructField, FloatType, StringType, TimestampType

BUCKET_NAME = os.getenv('BUCKET', 's3://apple-watch-activity-data')
SOURCE_DIR = os.getenv('PROCESSED_PATH', 'data/processed')
ARTIFACT_DIR = os.getenv('MODEL_PATH', 'model')
DB_NAME = os.getenv('DB_NAME', 'watchdata')
TABLE_NAME = os.getenv('TABLE_NAME', 'activitydata')


if __name__ == "__main__":

    spark = SparkSession.builder.appName("train-activity-model").getOrCreate()

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

    # load cleaned parquet data from s3
    data = spark \
        .read \
        .option("mode", "DROPMALFORMED") \
        .parquet(f"{os.getenv}/{SOURCE_DIR}")

    # Data Preprocessing

    # convert string to string-indexed variables using StringIndexer and OneHot
    string_cols = ['location_type', 'battery_state']
    string_indexer = StringIndexer(inputCols=string_cols, outputCols=[s + '_indexed' for s in string_cols],
                                   handleInvalid='keep')

    # transform
    data_indexed = string_indexer.fit(data).transform(data)

