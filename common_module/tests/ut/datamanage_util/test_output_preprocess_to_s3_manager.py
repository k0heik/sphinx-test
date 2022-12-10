import uuid

import pandas as pd
import boto3

from common_module.aws_util import s3_client
from common_module.datamanage_util import output_preprocess_to_s3_manager


class TestBusinessException:
    BUCKET_NAME = 'sophiaai-bid-optimisation-ml-test-2'

    @classmethod
    def setup_class(cls):
        cls._bucket_name = cls.BUCKET_NAME
        cls._client = s3_client()

        cls._create_s3_bucket()

    @classmethod
    def _create_s3_bucket(cls):
        try:
            cls._client.create_bucket(
                Bucket=cls._bucket_name,
                CreateBucketConfiguration={
                    'LocationConstraint': boto3.session.Session().region_name
                }
            )
        except Exception as e:
            if e.__class__.__name__ in [
               'BucketAlreadyExists', 'BucketAlreadyOwnedByYou']:
                pass
            else:
                raise e

    @classmethod
    def teardown_class(cls):
        cls._delete_all_data(cls._bucket_name)
        cls._client.delete_bucket(Bucket=cls._bucket_name)

    @classmethod
    def _delete_all_data(cls, bucket):
        next_token = ''
        contents_count = 0
        while True:
            if next_token == '':
                response = cls._client.list_objects_v2(Bucket=bucket)
            else:
                response = cls._client.list_objects_v2(
                    Bucket=bucket, ContinuationToken=next_token)

            if 'Contents' in response:
                contents = response['Contents']
                contents_count = contents_count + len(contents)
                for content in contents:
                    cls._client.delete_object(
                        Bucket=bucket, Key=content['Key'])

            if 'NextContinuationToken' in response:
                next_token = response['NextContinuationToken']
            else:
                break

    def test_output_preprocess_to_s3_manager(self):
        df = pd.DataFrame({column: [1] for column in "ABC"})

        s3_key = f"{uuid.uuid4()}"
        output_preprocess_to_s3_manager(
            s3_bucket=self._bucket_name,
            s3_key=s3_key,
        )(df)

        res = self._client.get_object(Bucket=self._bucket_name, Key=s3_key)
        assert res['Body'].read().decode('utf-8') == df.to_csv(index=False)
