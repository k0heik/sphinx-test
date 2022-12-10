import os
import uuid
import json
import pytest
import pandas as pd
from common_module.aws_util import (
    s3_client,
    load_file_list,
    write_df_to_s3,
    write_json_to_s3,
    read_binary_from_s3,
    read_from_s3,
    read_from_s3_uri,
)
from common_module.tests.testutil import (
    create_s3_bucket,
    delete_s3_bucket,
)


@pytest.mark.skipif(
    os.environ.get('PYTEST_WITH_EXTERNAL', 'no') != 'yes',
    reason='Skip because PYTEST_WITH_EXTERNAL is not "yes".')
class TestAWSUtil:
    @classmethod
    def setup_class(cls):
        cls._bucket_name = 'sophiaai-bid-optimisation-ml-test-2'
        cls._client = s3_client()

        create_s3_bucket(cls._client, cls._bucket_name)

    @classmethod
    def teardown_class(cls):
        delete_s3_bucket(cls._client, cls._bucket_name)

        cls._client.close()

    def _csv_index_header_pattern():
        return [
            (True, True),
            (True, False),
            (False, True),
            (False, False),
        ]

    @pytest.mark.parametrize('index, header', _csv_index_header_pattern())
    def test_write_df_to_s3(self, index, header):
        """
        write_df_to_s3関数のテスト
        """
        df = pd.get_dummies(pd.Series(['A', 'B', 'C']))
        key = f'{uuid.uuid4()}/{uuid.uuid4()}'
        write_df_to_s3(df, self._bucket_name, key, index, header)

        response = self._client.get_object(Bucket=self._bucket_name, Key=key)

        assert response['Body'].read().decode(
            'utf-8') == df.to_csv(index=index, header=header)

    def test_write_json_to_s3(self):
        """
        write_json_to_s3関数のテスト
        """
        json_data = json.dumps({f"{uuid.uuid4()}": f"{uuid.uuid4()}"})
        key = f'{uuid.uuid4()}/{uuid.uuid4()}'

        write_json_to_s3(json_data, self._bucket_name, key)

        response = self._client.get_object(Bucket=self._bucket_name, Key=key)

        assert response['Body'].read().decode('utf-8') == json_data

    def test_read_binary_from_s3(self):
        key = f'{uuid.uuid4()}/{uuid.uuid4()}'
        s3_body = f'{uuid.uuid4()}'

        self._client.put_object(
            Bucket=self._bucket_name, Key=key, Body=s3_body)

        assert read_binary_from_s3(self._bucket_name, key) == \
            s3_body.encode('utf-8')

    def test_read_from_s3(self, monkeypatch):
        """
        read_from_s3関数のテスト
        """
        key = f'{uuid.uuid4()}/{uuid.uuid4()}'

        self._client.put_object(
            Bucket=self._bucket_name,
            Key=key,
            Body=pd.get_dummies(pd.Series(['A', 'B', 'C'])).to_csv())

        assert isinstance(
            read_from_s3(self._bucket_name, key), pd.core.frame.DataFrame)

    def test_read_from_s3_uri(self, monkeypatch):
        """
        read_from_s3_uri関数のテスト
        """
        key = f'{uuid.uuid4()}/{uuid.uuid4()}'

        self._client.put_object(
            Bucket=self._bucket_name,
            Key=key,
            Body=pd.get_dummies(pd.Series(['A', 'B', 'C'])).to_csv())

        assert isinstance(
            read_from_s3_uri(f"s3://{self._bucket_name}/{key}"),
            pd.core.frame.DataFrame)

    def test_load_file_list(self):
        prefix = f'{uuid.uuid4()}/{uuid.uuid4()}'
        key = f'{prefix}/{uuid.uuid4()}'
        ng_key = f'{prefix}/ng/{uuid.uuid4()}'
        s3_body = f'{uuid.uuid4()}'

        self._client.put_object(
            Bucket=self._bucket_name, Key=key, Body=s3_body)
        self._client.put_object(
            Bucket=self._bucket_name, Key=ng_key, Body=s3_body)

        assert load_file_list(self._bucket_name, prefix) == [key]
