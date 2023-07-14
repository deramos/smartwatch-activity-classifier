# Smartwatch Activity Classifier
Module for staging, modeling, classifying, and generating new Apple SmartWatch Data. We will use AWS S3, AWS EMR, 
Kafka, Apache Spark, and Databricks Datagen.

## Data Loading
The first stage of the application is to load bulk data (~160GB) from Washington State University (WSU)
Center of Advanced Studies in Adaptive Systems (CASAS) archives 
 [https://casas.wsu.edu/datasets/smartwatch/](https://casas.wsu.edu/datasets/smartwatch/) into
a staging folder in an S3 bucket. We use a Spark job on EMR to clean the data (remove null values)
and load them into a glue table.

![](images/process-raw-data.png)


## Model Training
The cleaned up data is used to train a Classifier to predict the recorded activity using the watch raw
data. The notebook to perform this can be found here [Activity Classifier](activitymodule/spark/notebooks/Train%20Activity%20Classifier.ipynb)

The trained model will be used to predict the activity of new samples streamed from a kafka topic using spark streaming
later in the application. 

### Active Learning
To retrain the model with new samples, active learning technique is used. When new data samples are streamed from kafka 
topic, the activities on those samples are predicted using the model trained above. Misclassified samples are labeled with 
the correct class labels by a human annotator and saved to a feature store. Airflow routinely picks samples in the feature
store and retrains the model. If the new model has a better than the old one, it is used to classify new streaming samples

![](images/active-learning.png)

## Kafka Streaming
To simulate the generation of new Apple Watch Data, we will use [Databricks Labs DataGen](https://github.com/databrickslabs/dbldatagen)
and the descriptive statistics calculated from the raw data samples. This ensures that generated data maintains the 
distribution of the original dataset. 
The generated data is loaded into spark streaming and written into a kafka topic. The activity prediction pipeline above
picks up the data in the topics and makes predictions. The samples with low predictions are sent to the annotation pipeline 
that will be used to retrain the model at a later date. 

![](images/generate-new-data.png)

## TO RUN
To run, clone the project. Run the `init.sh` to fetch data from WSU and sync it to S3. The script also copies spark scripts 
from local directory to s3 bucket to be used later in EMR. It builds the project using `docker compose build`.

Run `rundatapreprocess.py` to submit an EMR job which creates a cluster (if it doesn't exist), and cleans up the watch 
data in S3. The cleaned data is saved into a Glue table and later migrated to a redshift table.

```
python activitymodule/jobs/run-data-preprocess.py
```
This job cleans up data from the staging directory on S3 and moves it to a redshift table (and a Glue catalog).

Run `train-activity-model.py` to  submit the EMR to train a RandomForestClassifier to predict watch activity using the 
raw data.

Start the project using `docker compose up` and exec into the `activitymodule_spark` container to submit the spark
streaming job, and the dbl datagen script

```
docker exec -it activitymodule_spark /bin/bash
/usr/lib/spark/bin/spark-submit /home/hadoop/spark/scripts/predict-activity.py
/usr/lib/spark/bin/spark-submit /home/hadoop/spark/scripts/generate-new-data.py
```
The `generate-new-data` script uses dblDataGen to generate new watch data that is of the same distribution as the previously
loaded watch data. This statistics of the data is loaded from glue or redshift. The newly generated data is sent into a 
kafka topic. 

The `predict-activity` script reads data from kafka topic and predicts the activity associated with the data.

```commandline
jupyter nbcovert --execute /home/hadoop/spark/notebooks/train-activity-classifier.ipynb
```

The saved model is loaded into the `predict-activity` notebook which also starts the spark streaming job. The spark stream
takes data from `ticker-watch-data` kafka topic which is constantly loaded with ticker data generated by the `dbl-data-gen`
notebook using Databricks dblDataGen

---
<br>

&copy; | [@deramos](https://github.com/deramos)
