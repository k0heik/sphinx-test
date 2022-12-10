import os
import boto3
from google.api_core import exceptions


def is_execute_external():
    return os.environ.get('PYTEST_WITH_EXTERNAL', 'no') == 'yes'


def reason_execute_external():
    return 'Skip because PYTEST_WITH_EXTERNAL is not "yes".'


def delete_all_s3_data(s3_client, bucket_name):
    next_token = ''
    contents_count = 0
    while True:
        if next_token == '':
            response = s3_client.list_objects_v2(Bucket=bucket_name)
        else:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name, ContinuationToken=next_token)

        if 'Contents' in response:
            contents = response['Contents']
            contents_count = contents_count + len(contents)
            for content in contents:
                s3_client.delete_object(
                    Bucket=bucket_name, Key=content['Key'])

        if 'NextContinuationToken' in response:
            next_token = response['NextContinuationToken']
        else:
            break


def create_s3_bucket(s3_client, bucket_name):
    try:
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': boto3.session.Session().region_name
            }
        )
    except Exception as e:
        if e.__class__.__name__ in ['BucketAlreadyExists', 'BucketAlreadyOwnedByYou']:
            pass
        else:
            raise e


def delete_s3_bucket(s3_client, bucket_name):
    delete_all_s3_data(s3_client, bucket_name)
    s3_client.delete_bucket(Bucket=bucket_name)


def create_gcs_bucket(storage_client, bucket_name):
    try:
        storage_client.create_bucket(bucket_name)
    except exceptions.Conflict:
        pass


def delete_gcs_bucket(storage_client, bucket_name):
    bucket = storage_client.get_bucket(bucket_name)
    bucket.delete(force=True)
