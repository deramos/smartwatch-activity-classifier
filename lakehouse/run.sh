##!/bin/bash
################################################################################################
##         Download Apple Watch Activity DData from CASAS Washington State University         ##
##         Upload the Data to a bucket on Amazon S3 Bucket. This bucket will be used to       ##
##         process the data using Spark scripts on AWS EMR using hadoop docker container      ##
################################################################################################

## make data directory and collect data groups
rootdatafol=/tmp/applewatchdata
if [[ ! -d "$rootdatafol" ]]
then
  mkdir "$rootdatafol"
else
  echo "root data folder exists ..."
fi

datagroups=(group1 group2 group3)

# iterate through data groups and download, extract data to a temp folder and copy to the data folder
for group in "${datagroups[@]}"; do
  file_path="https://casas.wsu.edu/datasets/smartwatch/$group.zip"
  echo "downloading $file_path..."
  curl -L "$file_path" -o "$rootdatafol/$group.zip"

  unzip "$rootdatafol/$group.zip" -d "$rootdatafol/$group"
  cp -r "$rootdatafol/$group/" ./data

done

## sync apple watch data to s3
aws s3 sync ./data/ s3://apple-watch-activity-data/staging/

## build emr docker image and start it using docker compose
docker compose build

## ssh into `lakehouse-spark` container and submit the spark-job file
docker exec lakehouse-spark ./submit_emr_job
