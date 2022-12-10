import os
import uuid
import time
import datetime
import io
import json
import pytest
from unittest.mock import MagicMock
from google.cloud import (
    storage,
    bigquery,
)
from google.oauth2 import service_account
import google
from common_module.aws_util import (
    s3_client,
    get_ssm_parameter,
    write_json_to_s3,
)
from common_module.bigquery_util import BigQueryService

from common_module.tests.testutil import (
    is_execute_external,
    reason_execute_external,
    create_s3_bucket,
    delete_s3_bucket,
    create_gcs_bucket,
    delete_gcs_bucket,
)


@pytest.mark.skipif(not is_execute_external(),
                    reason=reason_execute_external())
class TestBigQueryService:
    LOCATION = 'ASIA-NORTHEAST1'
    DATASET_NAME = 'bid_optimisation_ut'
    DUMMY_DATA_COLUMNS = ['A', 'B', 'C']
    DATE_PARTITION_COLUMN_NAME = 'date'

    @classmethod
    def setup_class(cls):
        cls._project_id = os.environ['GCP_PROJECT_ID']
        cls._dataset_name = cls.DATASET_NAME
        cls._table_name = f'{uuid.uuid4()}'
        cls._default_data_date = datetime.datetime.today()
        cls._bucket_name = 'sophiaai-bid-optimisation-ml-test-2'

        credentials = service_account.Credentials.from_service_account_info(
            json.loads(
                get_ssm_parameter(os.environ["SSM_GCP_KEY_PARAMETER_NAME"])))

        cls._bigquery_client = bigquery.Client(
            project=cls._project_id, credentials=credentials)
        cls._storage_client = storage.Client(
            project=cls._project_id, credentials=credentials)
        cls._s3_client = s3_client()

        cls._create_bigquery_dataset_and_data()
        create_gcs_bucket(cls._storage_client, cls._bucket_name)
        create_s3_bucket(cls._s3_client, cls._bucket_name)

    @classmethod
    def _create_bigquery_dataset_and_data(cls):
        dataset = bigquery.Dataset(f'{cls._project_id}.{cls._dataset_name}')
        dataset.location = cls.LOCATION
        try:
            dataset = cls._bigquery_client.create_dataset(dataset, timeout=30)
        except google.api_core.exceptions.Conflict:
            pass
        except Exception as e:
            raise (e)

        cls._write_test_data(cls._default_data_date)

    @classmethod
    def _write_test_data(cls, target_date):
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField(
                    x,
                    "STRING") for x in cls.DUMMY_DATA_COLUMNS] + [
                bigquery.SchemaField(
                    cls.DATE_PARTITION_COLUMN_NAME,
                    "DATE")],
            skip_leading_rows=0,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=cls.DATE_PARTITION_COLUMN_NAME,
                expiration_ms=1 * 24 * 60 * 60 * 100),
        )

        source_file = io.StringIO('{},{}'.format(
            ",".join(cls.DUMMY_DATA_COLUMNS),
            target_date.strftime("%Y-%m-%d")
        ))

        job = cls._bigquery_client.load_table_from_file(
            source_file,
            f'{cls._project_id}.{cls._dataset_name}.{cls._table_name}',
            job_config=job_config
        )
        job.result()

    @classmethod
    def teardown_class(cls):
        cls._bigquery_client.delete_dataset(
            cls._dataset_name, delete_contents=True, not_found_ok=False
        )

        delete_gcs_bucket(cls._storage_client, cls._bucket_name)
        delete_s3_bucket(cls._s3_client, cls._bucket_name)

        cls._bigquery_client.close()
        cls._storage_client.close()
        cls._s3_client.close()

    def test_init(self):
        """
        インスタンス生成時の処理成功
        """
        BigQueryService(self._project_id, self._dataset_name)

    def test_s3_to_gcs(self):
        instance = BigQueryService(self._project_id, self._dataset_name)
        key = f"{uuid.uuid4()}"
        data = f"{{{key}}}"

        write_json_to_s3(f"{{{key}}}", self._bucket_name, key)

        instance.s3_to_gcs(self._bucket_name, self._bucket_name, key)

        gcs_bucket = self._storage_client.get_bucket(self._bucket_name)
        dl_data = gcs_bucket.blob(key).download_as_bytes().decode()

        assert data == dl_data

    def test_delete_partition_success(self):
        """
        delete_partition関数の正常パターン
        """
        target_date = self._default_data_date + datetime.timedelta(days=3)

        assert 1 == self._get_test_table_partition_data_count(
            self._default_data_date)
        assert 0 == self._get_test_table_partition_data_count(target_date)

        self._write_test_data(target_date)
        assert 1 == self._get_test_table_partition_data_count(
            self._default_data_date)
        assert 1 == self._get_test_table_partition_data_count(target_date)

        instance = BigQueryService(self._project_id, self._dataset_name)
        instance.delete_partition(self._table_name, target_date)

        assert 1 == self._get_test_table_partition_data_count(
            self._default_data_date)
        assert 0 == self._get_test_table_partition_data_count(target_date)

    def test_delete_partition_no_partition(self):
        """
        delete_partition関数で対象パーティションが存在しない場合はエラーにならない
        """
        target_date = self._default_data_date + datetime.timedelta(days=3)

        assert 0 == self._get_test_table_partition_data_count(target_date)
        assert 1 == self._get_test_table_partition_data_count(
            self._default_data_date)

        instance = BigQueryService(self._project_id, self._dataset_name)
        instance.delete_partition(self._table_name, target_date)

        assert 0 == self._get_test_table_partition_data_count(target_date)
        assert 1 == self._get_test_table_partition_data_count(
            self._default_data_date)

    def test_delete_partition_no_table(self):
        """
        delete_partition関数で対象テーブルが存在しない場合はエラーになる
        """
        instance = BigQueryService(self._project_id, self._dataset_name)
        with pytest.raises(google.api_core.exceptions.NotFound):
            instance.delete_partition('dummy', self._default_data_date)

    def test_get_query(self, tmpdir):
        """_get_queryの正常系
        """
        p = tmpdir.mkdir("sql").join("sql.tpl")
        p.write("SELECT * FROM `{{ dataset_name }}`.`{{ table_name }}`;")
        instance = BigQueryService(self._project_id, self._dataset_name)
        params = {
            "dataset_name": self._dataset_name,
            "table_name": self._table_name,
        }
        sql = instance._get_query(tmpdir.join("sql"), "sql.tpl", params)
        assert sql == \
            f"SELECT * FROM `{self._dataset_name}`.`{self._table_name}`;"

    def test_append_from_df(self):
        """append_from_dfの正常系
        """
        instance = BigQueryService(self._project_id, self._dataset_name)
        before_count = self._get_test_table_data_count()

        append_df = instance.get_df_by_query(
            f"SELECT * FROM `{self._dataset_name}`.`{self._table_name}`")
        instance.append_from_df(self._table_name, append_df)

        assert before_count + len(append_df) == \
            self._get_test_table_data_count()

    def test_append_from_json(self):
        """append_from_jsonの正常系
        """
        instance = BigQueryService(self._project_id, self._dataset_name)
        before_count = self._get_test_table_data_count()

        append_df = instance.get_df_by_query(
            f"SELECT * FROM `{self._dataset_name}`.`{self._table_name}`")
        append_json = append_df.to_dict(orient="records")
        instance.append_from_json(
            self._dataset_name,
            self._table_name,
            json.loads(
                json.dumps(
                    append_json,
                    default=lambda x: x.isoformat() if hasattr(x, "isoformat") else x
                )
            )
        )

        assert before_count + len(append_df) == self._get_test_table_data_count()

    def test_create_table_from_df(self):
        """create_table_from_dfの正常系
        """
        instance = BigQueryService(self._project_id, self._dataset_name)

        append_df = instance.get_df_by_query(
            f"SELECT * FROM `{self._dataset_name}`.`{self._table_name}`")
        instance.create_table_from_df(self._dataset_name, self._table_name, append_df)

        assert len(append_df) == \
            self._get_test_table_data_count()

    def test_extract_to_df(self, tmpdir):
        """extract_to_dfの正常系
        """
        p = tmpdir.mkdir("sql").join("sql.tpl")
        p.write("SELECT * FROM `{{ dataset_name }}`.`{{ table_name }}`;")
        instance = BigQueryService(self._project_id, self._dataset_name)
        params = {
            "dataset_name": self._dataset_name,
            "table_name": self._table_name,
        }

        before_cache_file_num = self._count_cache_file_num()
        df = instance.extract_to_df(tmpdir.join("sql"), "sql.tpl", params)
        after_cache_file_num = self._count_cache_file_num()

        assert len(df) == self._get_test_table_data_count()
        assert after_cache_file_num == before_cache_file_num

    def test_extract_to_df_cache(self, tmpdir, mocker):
        """extract_to_dfの正常系
        """
        p = tmpdir.mkdir("sql").join("sql.tpl")
        p.write(
            "SELECT * FROM `{{ dataset_name }}`.`{{ table_name }}`"
            + "WHERE TRUE;")
        instance = BigQueryService(self._project_id, self._dataset_name)
        params = {
            "dataset_name": self._dataset_name,
            "table_name": self._table_name,
        }
        before_cache_file_num = self._count_cache_file_num()
        df = instance.extract_to_df(
            tmpdir.join("sql"), "sql.tpl", params, use_cache=True)
        after_cache_file_num = self._count_cache_file_num()

        assert len(df) == self._get_test_table_data_count()
        assert after_cache_file_num == before_cache_file_num + 1

        # キャッシュ済みの場合は増えない
        before_cache_file_num = self._count_cache_file_num()
        df = instance.extract_to_df(
            tmpdir.join("sql"), "sql.tpl", params, use_cache=True)
        after_cache_file_num = self._count_cache_file_num()

        assert after_cache_file_num == before_cache_file_num

        # キャッシュ済みの場合はpickle.loadの値が呼び出され、その戻り値が帰ってくることを確認
        mock_value = uuid.uuid4()
        pickemock = mocker.patch(
            'common_module.bigquery_util.bigqueryservice.pickle.load', return_value=mock_value)
        ret = instance.extract_to_df(
            tmpdir.join("sql"), "sql.tpl", params, use_cache=True)
        assert ret == mock_value
        pickemock.assert_called_once()

    @pytest.mark.parametrize("kwargs", [
        {"s3_bucket": "mybucket", "s3_key": "key.csv"},
        {"s3_uri": "s3://mybucket/key.csv"},
    ])
    def test_extract_to_s3(self,  mocker, tmpdir, kwargs):
        """extract_to_s3の正常系
        """
        p = tmpdir.mkdir("sql").join("sql.tpl")
        p.write("SELECT * FROM `{{ dataset_name }}`.`{{ table_name }}`;")
        instance = BigQueryService(self._project_id, self._dataset_name)
        params = {
            "dataset_name": self._dataset_name,
            "table_name": self._table_name,
        }

        write_df_to_s3 = MagicMock()
        mocker.patch("common_module.bigquery_util.bigqueryservice.write_df_to_s3", write_df_to_s3)
        df = instance.extract_to_s3(
            tmpdir.join("sql"), "sql.tpl", params, **kwargs)

        write_df_to_s3.assert_called_once()

        assert len(df) == self._get_test_table_data_count()

    def test_extract_to_s3_no_key_nor_uri(self, tmpdir):
        """extract_to_s3のs3keyやuriが指定されていない場合の異常系
        """
        p = tmpdir.mkdir("sql").join("sql.tpl")
        p.write("SELECT * FROM `{{ dataset_name }}`.`{{ table_name }}`;")
        instance = BigQueryService(self._project_id, self._dataset_name)
        params = {
            "dataset_name": self._dataset_name,
            "table_name": self._table_name,
        }
        with pytest.raises(ValueError):
            instance.extract_to_s3(tmpdir.join("sql"), "sql.tpl", params)

    def test_extract_to_integer_range_partition_table(self, tmpdir):
        """extract_to_integer_range_partition_tableの正常系
        """
        p = tmpdir.mkdir("sql").join("sql.tpl")
        p.write(
            f"SELECT {','.join(self.DUMMY_DATA_COLUMNS)}, 1 AS id "
            "FROM `{{ dataset_name }}`.`{{ table_name }}`;"
        )
        instance = BigQueryService(self._project_id, self._dataset_name)
        instance.extract_to_s3 = MagicMock()
        params = {
            "dataset_name": self._dataset_name,
            "table_name": self._table_name,
        }
        destination_table = \
            f"{self._table_name}_{str(uuid.uuid4()).replace('-', '_')}"
        job_id = instance.extract_to_integer_range_partition_table(
            tmpdir.join("sql"), "sql.tpl", params,
            self._dataset_name,
            destination_table,
            partition_field="id",
            min_id=1,
            max_id=100,
            interval=1
        )
        job = instance.get_query_job(job_id)
        job.result()  # to wait for the job to complete

        sql = \
            "SELECT * FROM " + \
            f" `{self._dataset_name}`.`{destination_table}`;"
        df = self._bigquery_client.query(sql).to_dataframe()
        assert len(df) == self._get_test_table_data_count()

        instance.extract_to_s3.assert_not_called()

    def _count_cache_file_num(self):
        cache_dir = BigQueryService.CACHE_DIR

        if not os.path.exists(cache_dir):
            return 0

        return len([name for name in os.listdir(cache_dir)
                    if os.path.isfile(os.path.join(cache_dir, name))])

    def _get_test_table_data_count(self):
        sql = 'SELECT COUNT(*)' + \
            f'FROM `{self._dataset_name}`.`{self._table_name}`'
        query = self._bigquery_client.query(sql)
        result = query.result()

        for row in result:
            return row[0]

    def _get_test_table_partition_data_count(self, target_date):
        sql = 'SELECT COUNT(*)' + \
            f'FROM `{self._dataset_name}`.`{self._table_name}`' + \
            f'WHERE `{self.DATE_PARTITION_COLUMN_NAME}` ' + \
            f'        = "{target_date.strftime("%Y-%m-%d")}"'
        query = self._bigquery_client.query(sql)
        result = query.result()

        for row in result:
            return row[0]

    def test_create_table(self):
        instance = BigQueryService(self._project_id, self._dataset_name)

        create_table_name = f"{self._table_name}_{time.time_ns()}"

        schema = instance.client.get_table(
            f"{self._dataset_name}.{self._table_name}").schema
        instance.create_table(self._dataset_name, create_table_name, schema)

        # 既存と同名テーブルを作成しようとするとエラー
        with pytest.raises(google.api_core.exceptions.Conflict):
            instance.create_table(self._dataset_name, create_table_name, schema)

    @pytest.mark.parametrize("is_exist", [True, False])
    def test_recreate_table(self, is_exist):
        instance = BigQueryService(self._project_id, self._dataset_name)
        schema = instance.client.get_table(
            f"{self._dataset_name}.{self._table_name}").schema
        table_name = f"{self._table_name}_{time.time_ns()}"
        if is_exist:
            instance.delete_table(self._dataset_name, table_name)
            instance.create_table(self._dataset_name, table_name, schema)
        else:
            instance.delete_table(self._dataset_name, table_name)

        instance.recreate_table(self._dataset_name, table_name, schema)

        try:
            instance.client.get_table(f"{self._dataset_name}.{self._table_name}")
            assert True
        except Exception:
            assert False
