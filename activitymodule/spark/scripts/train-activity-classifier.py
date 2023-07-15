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

