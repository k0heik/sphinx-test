import os
import io
import datetime
import uuid
import random

import json
import pandas as pd
import pytest
import boto3
import google
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from common_module.aws_util import (
    s3_client,
    get_ssm_parameter,
)
from common_module.bigquery_util.funcs import load_output_table_schema


from lambda_handler import lambda_handler, _TABLE_NAME_MAP


class TestRecordToBq:
    PROJECT_ID = 'sophiaai-develop'
    DATASET_NAME = f'{os.environ["TEST_CLOUD_RESOURCE_PREFIX"]}_optimise_bidding_ml_record_to_bq_test'
    S3_BUCKET_NAME = f'{os.environ["TEST_CLOUD_RESOURCE_PREFIX"]}-optimise-bidding-ml-record-result-test-s3-bucket'
    GCS_BUCKET_NAME = f'{os.environ["TEST_CLOUD_RESOURCE_PREFIX"]}-optimise-bidding-ml-record-result-test-gcs-bucket'
    CREDENTIAL_PARAMETER_NAME = '/SOPHIAAI/DWH/GCP/ACCESS_KEY/DEVELOP'
    LOCATION = 'ASIA-NORTHEAST1'

    credentials = None

    @classmethod
    def setup_class(cls):
        cls._table_name = f'{uuid.uuid4()}'

        value = get_ssm_parameter(cls.CREDENTIAL_PARAMETER_NAME)
        cls.credentials = service_account.Credentials.from_service_account_info(
            json.loads(value))

        cls._bigquery_client = bigquery.Client(
            project=cls.PROJECT_ID, credentials=cls.credentials)
        cls._create_bigquery_resources()

        cls._create_storage_resources()

        cls._s3_client = s3_client()
        cls._create_s3_resources()

    # storage_clientのみ使用都度取得
    # 使用間隔が空いていると接続が切れてエラーになってしまう様子のため
    @classmethod
    def _storage_client(cls):
        return storage.Client(project=cls.PROJECT_ID, credentials=cls.credentials)

    @classmethod
    def _create_bigquery_resources(cls):
        dataset = bigquery.Dataset(f'{cls.PROJECT_ID}.{cls.DATASET_NAME}')
        dataset.location = cls.LOCATION

        try:
            cls._bigquery_client.create_dataset(dataset, timeout=30)
        except google.api_core.exceptions.Conflict:
            pass
        except Exception as e:
            raise e

    @classmethod
    def _create_storage_resources(cls):
        try:
            cls._storage_client().create_bucket(
                cls.GCS_BUCKET_NAME, timeout=30, location=cls.LOCATION)
        except google.api_core.exceptions.Conflict:
            pass
        except Exception as e:
            raise e

    @classmethod
    def _create_s3_resources(cls):
        try:
            cls._s3_client.create_bucket(
                Bucket=cls.S3_BUCKET_NAME,
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

    def setup_method(self):
        self._delete_all_file(self.S3_BUCKET_NAME)
        for (result_table_name, _) in _TABLE_NAME_MAP.values():
            table_id = f"{self.PROJECT_ID}.{self.DATASET_NAME}.{result_table_name}"
            self._bigquery_client.delete_table(table_id, not_found_ok=True)

            table = bigquery.Table(
                table_id,
                schema=load_output_table_schema(result_table_name)
            )
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="date",
            )
            self._bigquery_client.create_table(table)

    @classmethod
    def teardown_class(cls):
        cls._bigquery_client.delete_dataset(
            cls.DATASET_NAME, delete_contents=True, not_found_ok=False
        )

        bucket = cls._storage_client().get_bucket(cls.GCS_BUCKET_NAME)
        bucket.delete(force=True)

        cls._delete_all_file(cls.S3_BUCKET_NAME)
        cls._s3_client.delete_bucket(Bucket=cls.S3_BUCKET_NAME)

        cls._bigquery_client.close()
        cls._s3_client.close()

    @classmethod
    def _delete_all_file(cls, bucket):
        next_token = ''
        contents_count = 0
        while True:
            if next_token == '':
                response = cls._s3_client.list_objects_v2(Bucket=bucket)
            else:
                response = cls._s3_client.list_objects_v2(
                    Bucket=bucket, ContinuationToken=next_token)

            if 'Contents' in response:
                contents = response['Contents']
                contents_count = contents_count + len(contents)
                for content in contents:
                    cls._s3_client.delete_object(
                        Bucket=bucket, Key=content['Key'])

            if 'NextContinuationToken' in response:
                next_token = response['NextContinuationToken']
            else:
                break

    @pytest.mark.parametrize("date", ["latest", "2020-10-15"])
    @pytest.mark.parametrize("output_csv_prefix", ["", "test"])
    def test_record_to_bq(self, monkeypatch, date, output_csv_prefix):
        monkeypatch.setenv('GCP_PROJECT_ID', self.PROJECT_ID)
        monkeypatch.setenv('SSM_GCP_KEY_PARAMETER_NAME', self.CREDENTIAL_PARAMETER_NAME)
        monkeypatch.setenv('OUTPUT_CSV_BUCKET', self.S3_BUCKET_NAME)
        monkeypatch.setenv('OUTPUT_CSV_PREFIX', output_csv_prefix)
        monkeypatch.setenv('DATASET_NAME', self.DATASET_NAME)
        monkeypatch.setenv('GCS_BUCKET', self.GCS_BUCKET_NAME)

        date_separater = "-"
        target_date_str = datetime.datetime.now().strftime(
            f"%Y{date_separater}%m{date_separater}%d") if date == "latest" else date

        yyyy, mm, dd = target_date_str.split(date_separater)

        expected_df_dict = {}
        for type, (result_table_name, _) in _TABLE_NAME_MAP.items():
            dir = os.path.join(
                output_csv_prefix,
                f"{yyyy}/{mm}/{dd}/{type}"
            )

            self._create_s3_csv(f"{dir}/ng/{uuid.uuid4()}", result_table_name, target_date_str)
            self._create_s3_csv(f"ng/{uuid.uuid4()}", result_table_name, target_date_str)

            expected_df = pd.concat([
                self._create_s3_csv(f"{dir}/{uuid.uuid4()}", result_table_name, target_date_str)
                for _ in range(random.randint(1, 3))
            ], axis=0)

            expected_df_dict[type] = expected_df

        lambda_handler({"date": date}, None)

        for type, (result_table_name, log_table_name_prefix) in _TABLE_NAME_MAP.items():
            result_df = self._extract_result_table_data(result_table_name, yyyy, mm, dd)
            assert len(result_df) == len(expected_df_dict[type])
            assert "test_only_log_column" not in result_df.columns

            log_df = self._extract_log_table_data(log_table_name_prefix, yyyy, mm, dd)
            assert len(log_df) == len(expected_df_dict[type])
            assert "test_only_log_column" in log_df.columns

    def test_record_to_bq_no_dir(self, monkeypatch):
        monkeypatch.setenv('GCP_PROJECT_ID', self.PROJECT_ID)
        monkeypatch.setenv('SSM_GCP_KEY_PARAMETER_NAME', self.CREDENTIAL_PARAMETER_NAME)
        monkeypatch.setenv('OUTPUT_CSV_BUCKET', self.S3_BUCKET_NAME)
        monkeypatch.setenv('DATASET_NAME', self.DATASET_NAME)
        monkeypatch.setenv('GCS_BUCKET', self.GCS_BUCKET_NAME)

        target_date_str = "2020-10-16"
        date = target_date_str

        lambda_handler({"date": date}, None)

    def _create_s3_csv(self, key, schema_name, target_date_str):
        df = self._df_cast(
            self._generate_testdata_df(schema_name, random.randint(1, 3), target_date_str)
        )
        f = io.StringIO(df.to_csv(index=False))
        self._s3_client.upload_fileobj(
            io.BytesIO(f.getvalue().encode()), self.S3_BUCKET_NAME, f"{key}.csv")

        return df

    def _generate_testdata_df(self, schema_name, num, target_date_str):
        bq_float_max_digit = 10
        float_over_zero = 7
        float_under_zero = bq_float_max_digit - float_over_zero

        value_map = {
            "INT64": random.randint(1, 10**10),
            "FLOAT64": round(random.random() * (10**float_over_zero), float_under_zero),
            "DATE": target_date_str,
            "BOOL": True,
            "STRING": "test",
        }
        df = pd.DataFrame({
            d["name"]: [value_map[d["type"]]] * num
            for d in load_output_table_schema(schema_name)})
        df["portfolio_id"] = None
        df["test_only_log_column"] = 1.0

        return self._df_cast(df)

    def _extract_result_table_data(self, result_table_name, yyyy, mm, dd):
        try:
            df = self._bigquery_client.query(
                f"SELECT * FROM `{self.DATASET_NAME}`.`{result_table_name}` WHERE `date` = '{yyyy}-{mm}-{dd}'"
            ).to_dataframe()
            return self._df_cast(df)

        except google.api_core.exceptions.NotFound:
            return None

    def _extract_log_table_data(self, log_table_name_prefix, yyyy, mm, dd):
        log_table_name = f"{log_table_name_prefix}_{yyyy}{mm}{dd}"
        try:
            df = self._bigquery_client.query(
                f"SELECT * FROM `{self.DATASET_NAME}`.`{log_table_name}` WHERE `date` = '{yyyy}-{mm}-{dd}'"
            ).to_dataframe()
            return self._df_cast(df)

        except google.api_core.exceptions.NotFound:
            return None

    def _df_cast(self, df):
        df["portfolio_id"] = df["portfolio_id"].astype("Int64")

        return df
