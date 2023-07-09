# Smartwatch AWS Lakehouse
Lakehouse for staging and modeling Apple smartwatch data on AWS S3 using spark and airflow

## Data Loading
The first stage of the application is to load bulk data from Washington State University (WSU)
Center of Advanced Studies in Adaptive Systems (CASAS) archives 
 [https://casas.wsu.edu/datasets/smartwatch/](https://casas.wsu.edu/datasets/smartwatch/) into
a staging folder in an S3 bucket. We use a Spark job on EMR to clean the data (remove null values)
and load them into a glue table.

![](images/process-raw-data.png)


## Model Training
The cleaned up data is used to train a Classifier to predict the recorded activity using the watch raw
data. 