import json
import boto3

glue = boto3.client('glue')

def lambda_handler(event, context):
    print("S3 Trigger received")

    job_name = "ecommerce-etl-job"   

    try:
        response = glue.start_job_run(JobName=job_name)
        print("Glue job started:", response['JobRunId'])
    except Exception as e:
        print("Error starting Glue job:", str(e))

    return {
        'statusCode': 200
    }