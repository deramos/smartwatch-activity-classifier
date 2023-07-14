# Use boto3 and EMR to run a spark job under ../spark/scripts on AWS
import boto3
import logging
from botocore.client import ClientError

emr_client = boto3.client('emr', region_name='us-east-1')

logger = logging.getLogger('activitymodule.jobs.rundataprocess')


def run_job_flow(_jobflow_role, _service_role, _job_steps):
    """
    Use emr client to create a cluster and run the staging to pre
    """
    try:
        response = emr_client.run_job_flow(
            Name="Load data from staging to processed",
            ReleaseLabel='emr-6.11.0',
            LogUri=log_uri,
            Configurations=[
                {
                    "Classification": "spark-env",
                    "Configurations": [
                        {
                            "Classification": "export",
                            "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                        }
                    ]
                }
            ],
            Instances={
                "InstanceGroups": [
                    {
                        "Name": "Master Node",
                        "Market": "SPOT",
                        "InstanceRole": "MASTER",
                        "InstanceType": "m4.xlarge",
                        "InstanceCount": 1,
                    },
                    {
                        "Name": "Core Nodes",
                        "Market": "SPOT",
                        "InstanceRole": "CORE",
                        "InstanceType": "m4.xlarge",
                        "InstanceCount": 2,
                    },
                ]
            },
            Steps=_job_steps,
            Applications=[{"Name": "Hadoop"}, {"Name": "Spark"}],
            JobFlowRole=_jobflow_role,
            ServiceRole=_service_role,
            EbsRootVolumeSize=20,
            VisibleToAllUsers=True
        )
        cluster_id = response['JobFlowId']
        logger.info("Created cluster %s.", cluster_id)
    except ClientError:
        logger.exception("Couldn't run job.")
        raise
    else:
        return cluster_id


if __name__ == "__main__":
    bucket_name = 'apple-watch-activity-data'
    script_name_path = 'code/scripts/staging-to-processed.py'

    log_uri = f's3://{bucket_name}/logs'

    job_flow_role = 'EMR_EC2_DefaultRole'
    service_role = 'EMR_DefaultRole'

    steps = [
        {
            "Name": "Process data from staging",
            "ActionOnFailure": "TERMINATE_CLUSTER",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "client",
                    f"s3://{bucket_name}/{script_name_path}",
                ],
            },
        }
    ]

    # run the job flow
    run_job_flow(
        _jobflow_role=job_flow_role,
        _service_role=service_role,
        _job_steps=steps
    )

