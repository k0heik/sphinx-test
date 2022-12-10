import os
import datetime
import traceback
import json
import hashlib
import pickle
import pandas as pd
from typing import List, Dict, Any, Optional, Callable
from io import BytesIO
from google.cloud import (
    storage,
    bigquery,
)
from google.oauth2 import service_account
from jinja2 import Environment, FileSystemLoader

from common_module.aws_util import (
    s3_client,
    get_ssm_parameter,
    write_df_to_s3,
    get_bucket_key_from_s3_uri,
)
from common_module.logger_util import get_custom_logger

logger = get_custom_logger()


class BigQueryService:

    CACHE_DIR = "/tmp/bid_optimisation_ml/bq_cache"

    def _get_gcp_key(self) -> dict:
        """SSMからGCPのクレデンシャルを取得する

        Returns:
            dict: gcpのクレデンシャル
        """
        parameter_name = os.environ["SSM_GCP_KEY_PARAMETER_NAME"]
        value = get_ssm_parameter(parameter_name)
        return json.loads(value)

    def __init__(self, project_id: str, dataset_name: str):
        """
        Args:
            dataset_name (str): データを登録するBigQueryのデータセット名
            table_name (str): データを登録するBigQueryのテーブル名
        """
        self._project = project_id
        self._dataset_name = dataset_name

        credentials = service_account.Credentials.from_service_account_info(
            self._get_gcp_key())

        self._storage_client = storage.Client(
            project=self._project,
            credentials=credentials)
        self._bigquery_client = bigquery.Client(
            project=self._project,
            credentials=credentials)

    def __del__(self):
        self._storage_client.close()
        self._bigquery_client.close()

    @property
    def client(self):
        return self._bigquery_client

    def s3_to_gcs(
            self,
            s3_bucket_name: str,
            gcs_bucket_name: str,
            key: str) -> List[str]:

        gcs_bucket = self._storage_client.get_bucket(gcs_bucket_name)
        try:
            inFileObj = BytesIO()
            s3_client().download_fileobj(s3_bucket_name, key, inFileObj)
            blob = gcs_bucket.blob(key)
            blob.upload_from_file(inFileObj, rewind=True)
        except Exception as e:
            logger.error(str(e))
            logger.error(key)
            logger.error(traceback.format_exc())
            raise e

        return key

    def append_gcs_to_bq(self, table_name, gcs_bucket_name: str, key: str, dataset_name: str = None):
        """gcsからBigQueryに指定したキーのファイルのデータを追加するメソッド

        Args:
            keys (str): 追加するファイルのキー
        """
        if dataset_name is None:
            dataset_name = self._dataset_name
        table_id = f"{self._project}.{dataset_name}.{table_name}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        )

        uri = f"gs://{gcs_bucket_name}/{key}"

        load_job = self._bigquery_client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )

        load_job.result()

    def get_df_by_query(self, sql: str, use_cache: bool = False):
        """BQに対してSQLを実行し、結果をpandas.DataFrameで返す

        Args:
            sql (str): 実行するSQL
        """
        cache_key = hashlib.md5(sql.encode()).hexdigest()
        cache_path = f"{self.CACHE_DIR}/{cache_key}.pkl"

        if use_cache and os.path.exists(cache_path):
            logger.info("using cache")
            with open(cache_path, 'rb') as f:
                return pickle.load(f)

        df = self._bigquery_client.query(sql).to_dataframe()

        if use_cache:
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            with open(cache_path, 'wb') as f:
                pickle.dump(df, f)

        return df

    def delete_partition(self, table_name: str, key_date: datetime):
        table_id = f'{self._project}.{self._dataset_name}.{table_name}'
        key = f'{key_date.year}{key_date.month:02}{key_date.day:02}'

        self._bigquery_client.delete_table(f'{table_id}${key}')

    def _get_query(self, template_root_dir: str,
                   filename: str, parameters: Dict[str, Any]):

        env = Environment(
            loader=FileSystemLoader(
                template_root_dir,
                encoding='utf8',
            )
        )
        tpl = env.get_template(filename)

        return tpl.render(parameters)

    def append_from_query_tpl(
        self, template_root_dir: str,
        filename: str, parameters: Dict[str, Any],
        table_name: str
    ):
        sql = self._get_query(
            template_root_dir,
            filename,
            parameters
        )
        return self.append_from_query(
            sql=sql,
            table_name=table_name
        )

    def append_from_query(self, table_name, sql: str):
        table_id = f"{self._project}.{self._dataset_name}.{table_name}"
        job_config = bigquery.QueryJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            destination=table_id
        )

        query_job = self._bigquery_client.query(
            sql, job_config=job_config)
        query_job.result()

    def append_from_df(self, table_name, df):
        table_id = f"{self._project}.{self._dataset_name}.{table_name}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        job = self._bigquery_client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )

        job.result()

    def create_table_from_df(
        self,
        destination_dataset_name: str,
        table_name: str,
        df: "pd.DataFrame"
    ):
        """pandas.DataFrameからtableを新規作成を上書きで作成する
        """
        table_id = f"{self._project}.{destination_dataset_name}.{table_name}"

        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        job = self._bigquery_client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )

        job.result()

    def append_from_json(
        self,
        destination_dataset_name: str,
        table_name: str,
        json_object: List[Dict[str, Any]],
        schema: Optional[Dict[str, Any]] = None,
    ):
        table_id = f"{self._project}.{destination_dataset_name}.{table_name}"

        job_config = bigquery.LoadJobConfig(
            autodetect=schema is None,
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        job = self._bigquery_client.load_table_from_json(
            json_object, table_id, job_config=job_config
        )

        job.result()

    def query_export_table(
        self, destination_dataset_name: str, table_name: str, sql: str,
        partition_field: str = None, range_start: int = 0,
        range_end: int = None, interval: int = 1
    ) -> str:
        table_id = f"{self._project}.{destination_dataset_name}.{table_name}"

        if partition_field is not None:
            range_partitioning = bigquery.RangePartitioning(
                field=partition_field,
                range_=bigquery.PartitionRange(
                    start=range_start, end=range_end, interval=interval),
            )
            job_config = bigquery.QueryJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                destination=table_id,
                range_partitioning=range_partitioning
            )
        else:
            job_config = bigquery.QueryJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                destination=table_id
            )

        query_job = self._bigquery_client.query(
            sql, job_config=job_config)
        return query_job.job_id

    def get_query_job(self, job_id: str, location: str = "asia-northeast1"):
        job = self._bigquery_client.get_job(job_id, location=location)

        return job

    def extract_to_df(self, template_root_dir: str,
                      filename: str, parameters: Dict[str, Any],
                      use_cache: bool = False):
        """bqに対するsql実行結果をpd.DataFrameで返す

        Args:
            template_root_dir (str): SQLテンプレートファイルディレクトリ
            filename (str): SQLファイル名
            parameters (Dict[str, Any]): テンプレートに与えるパラメータ辞書

        Returns:
            pd.DataFrame: sql実行結果
        """
        sql = self._get_query(
            template_root_dir,
            filename,
            parameters,
        )
        return self.get_df_by_query(sql=sql, use_cache=use_cache)

    def extract_to_s3(self, template_root_dir: str,
                      filename: str, parameters: Dict[str, Any], *,
                      s3_bucket: Optional[str] = None,
                      s3_key: Optional[str] = None,
                      s3_uri: Optional[str] = None,
                      callback: Optional[Callable] = None,
                      ):
        """bqに対するsql実行結果をs3へ保存しpd.DataFrameで返す

        Args:
            template_root_dir (str): SQLテンプレートファイルディレクトリ
            filename (str): SQLファイル名
            parameters (Dict[str, Any]): テンプレートに与えるパラメータ辞書
            s3_bucket (str): s3 bucket (s3_keyと同時に指定・s3_uriと排他)
            s3_key (str): s3 key (s3_bucketと同時に指定・s3_uriと排他)
            s3_uri (str): s3 uri (単独で指定・s3_bucket, s3_keyと排他)
            callback (Callable): s3に出力する前にdfに対して適用する関数

        Returns:
            pd.DataFrame: sql実行結果
        """
        if s3_bucket and s3_key:
            pass
        elif s3_uri:
            s3_bucket, s3_key = get_bucket_key_from_s3_uri(s3_uri)
        else:
            raise ValueError(
                "One of tuple of s3_bucket and s3_key "
                "or s3_uri must be speficed!"
            )

        df = self.extract_to_df(
            template_root_dir,
            filename,
            parameters,
        )
        if callback:
            df = callback(df)

        write_df_to_s3(df, s3_bucket, s3_key)

        return df

    def delete_table(self, dataset_name, table):
        table_id = f"{self._project}.{dataset_name}.{table}"
        self._bigquery_client.delete_table(table_id, not_found_ok=True)

    def create_table(self, dataset_name: str, table_name: str, schema: list):
        table_id = f"{self._project}.{dataset_name}.{table_name}"

        table = bigquery.Table(table_id, schema=schema)
        self._bigquery_client.create_table(table)

    def recreate_table(self, dataset_name: str, table_name: str, schema: list):
        self.delete_table(dataset_name, table_name)
        self.create_table(dataset_name, table_name, schema)

    def extract_to_integer_range_partition_table(
        self,
        template_root_dir: str,
        filename: str,
        parameters: Dict[str, Any],
        destination_dataset_name: str,
        table_name: str,
        partition_field: str,
        min_id: int,
        max_id: int,
        interval: int,
    ):
        """bqに対するsql実行結果をテーブルに出力し，job_idを返す

        Args:
            template_root_dir (str): SQLテンプレートファイルディレクトリ
            filename (str): SQLファイル名
            parameters (Dict[str, Any]): テンプレートに与えるパラメータ辞書
            destination_dataset_name (str): 出力先のデータセット
            table_name (str): 出力先のテーブル
            partition_field (str): パーティション列名
            min_id (int): パーティションidの最小値
            max_id (int): パーティションidの最大値
            interval (int): パーティションを切るidの感覚
        """

        sql = self._get_query(
            template_root_dir,
            filename,
            parameters,
        )

        return self.query_export_table(
            destination_dataset_name=destination_dataset_name,
            table_name=table_name,
            sql=sql,
            partition_field=partition_field,
            range_start=min_id,  # (inclusive)
            range_end=max_id + 1,  # (exclusive)
            interval=interval
        )
