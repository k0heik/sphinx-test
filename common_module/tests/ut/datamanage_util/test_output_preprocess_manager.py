import uuid
import time
import json

import pandas as pd
import boto3
import google
from google.cloud import bigquery
from google.oauth2 import service_account

from common_module.aws_util import (
    s3_client,
    get_ssm_parameter
)
from common_module.bigquery_util import BigQueryService

from common_module.datamanage_util import output_preprocess_manager


class TestBusinessException:
    GCP_PROJECT_ID = 'sophiaai-develop'
    SSM_GCP_KEY_PARAMETER_NAME = "/SOPHIAAI/DWH/GCP/ACCESS_KEY/DEVELOP"
    LOCATION = 'ASIA-NORTHEAST1'
    DATASET_NAME = 'bid_optimisation_ut'
    BUCKET_NAME = 'sophiaai-bid-optimisation-ml-test-2'

    @classmethod
    def setup_class(cls):
        cls._project_id = cls.GCP_PROJECT_ID
        cls._dataset_name = cls.DATASET_NAME
        cls._bucket_name = cls.BUCKET_NAME
        cls._client = s3_client()

        credentials = service_account.Credentials.from_service_account_info(
            json.loads(
                get_ssm_parameter(cls.SSM_GCP_KEY_PARAMETER_NAME)))

        cls._bigquery_client = bigquery.Client(
            project=cls._project_id, credentials=credentials)

        cls._create_s3_bucket()
        cls._create_bigquery_dataset()

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
    def _create_bigquery_dataset(cls):
        dataset = bigquery.Dataset(f'{cls._project_id}.{cls._dataset_name}')
        dataset.location = cls.LOCATION
        try:
            dataset = cls._bigquery_client.create_dataset(dataset, timeout=30)
        except google.api_core.exceptions.Conflict:
            pass
        except Exception as e:
            raise (e)

    @classmethod
    def teardown_class(cls):
        cls._bigquery_client.delete_dataset(
            cls._dataset_name, delete_contents=True, not_found_ok=False
        )

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

    def test_output_preprocess_manager(self, monkeypatch):
        monkeypatch.setenv("SSM_GCP_KEY_PARAMETER_NAME", self.SSM_GCP_KEY_PARAMETER_NAME)
        monkeypatch.setenv("GCP_PROJECT_ID", self.GCP_PROJECT_ID)

        df = pd.DataFrame({column: [1] for column in "ABC"})
        bq = BigQueryService(self._project_id, self._dataset_name)

        s3_key = f"{uuid.uuid4()}"
        bq_table_name = f"test_table_{int(time.time())}"
        output_preprocess_manager(
            s3_bucket=self._bucket_name,
            s3_key=s3_key,
            bq=bq,
            bq_dataset_name=self._dataset_name,
            bq_table_name=bq_table_name,
        )(df)

        res = self._client.get_object(Bucket=self._bucket_name, Key=s3_key)
        assert res['Body'].read().decode('utf-8') == df.to_csv(index=False)

        res = bq.get_df_by_query(
            f"SELECT * FROM {self._dataset_name}.{bq_table_name}")
        pd.testing.assert_frame_equal(res, df)
