from concurrent.futures import ThreadPoolExecutor as TPE
from concurrent.futures import wait
import dataclasses as dc
import datetime
import glob
import json
import logging
import os
import random
import re
import socket
import sys
import tempfile
import time
import io
import copy
import uuid
from functools import lru_cache

import boto3
import invoke
import numpy as np
import pandas as pd
import requests
import yaml
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

from utils import Converter, TestCase
from build_targets import BUILD_TARGETS

logging.basicConfig(level=logging.INFO)
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)

DATASET = os.getenv("DATASET_NAME")
DATASET_COMMERCE_FLOW = os.getenv("COMMERCE_FLOW_DATASET_NAME")
SSM_GCP_KEY_PARAMETER_NAME = os.getenv("SSM_GCP_KEY_PARAMETER_NAME")
DEPLOY_KEY_PARAMETER_NAME = os.getenv("DEPLOY_KEY_PARAMETER_NAME")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_PROJECT_ID_SCHEMA = os.getenv("GCP_PROJECT_ID_SCHEMA")
DATASET_NAME_SCHEMA = os.getenv("DATASET_NAME_SCHEMA")
COMMERCE_FLOW_DATASET_NAME_SCHEMA = os.getenv(
    "COMMERCE_FLOW_DATASET_NAME_SCHEMA")
SSM_GCP_KEY_PARAMETER_NAME_SCHEMA = os.getenv(
    "SSM_GCP_KEY_PARAMETER_NAME_SCHEMA"
)
SSM_TARGET_UNIT_PARAMETER_NAME = os.getenv(
    "SSM_TARGET_UNIT_PARAMETER_NAME"
)
MY_TABLE_SCHEMA_LIST_DIR = os.getenv("MY_TABLE_SCHEMA_LIST_DIR")


def _minio_docker_env_options():
    return f"""\
        -e \"MINIO_URL={os.environ.get('MINIO_URL', "")}\" \\
        -e \"MINIO_ROOT_USER={os.environ.get('MINIO_ROOT_USER', "")}\" \\
        -e \"MINIO_ROOT_PASSWORD={os.environ.get('MINIO_ROOT_PASSWORD', "")}\" \\
    """


def _minio_params():
    MINIO_URL = os.environ.get('MINIO_URL', None)
    if MINIO_URL is None or MINIO_URL == "":
        return {}

    return {
        "endpoint_url": MINIO_URL,
        "aws_access_key_id": os.environ['MINIO_ROOT_USER'],
        "aws_secret_access_key": os.environ['MINIO_ROOT_PASSWORD'],
    }


def _s3_client():
    return boto3.client('s3', **_minio_params())


def _s3_resource():
    return boto3.resource('s3', **_minio_params())


def set_seed(seed=42):
    random.seed(seed)
    np.random.seed(seed)


def get_free_tcp_port():
    """空いているtcpポートを返す関数"""
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.bind(("", 0))
    addr, port = tcp.getsockname()
    tcp.close()
    return port


def get_ssm_parameter(parameter_name: str) -> str:
    """指定したパラメータ名の値をSSMから取得して返す関数"""
    ssm = boto3.client("ssm")
    response = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
    return response["Parameter"]["Value"]


def write_df_to_s3(df, s3_uri, index=False, header=True):
    """指定のデータフレームをCSV形式でS3に書き出す
    """
    m = re.match("^s3://(.+?)/(.+)$", s3_uri)
    bucket, key = m.groups()

    _s3_client().put_object(
        Bucket=bucket,
        Key=key,
        Body=df.to_csv(index=index, header=header)
    )


def create_s3_file(str, bucket, key):
    """指定のバケット・keyにファイルを作成する
    """
    _s3_client().upload_fileobj(
        io.BytesIO(io.StringIO(str).read().encode('utf8')), bucket, key)


def get_client():
    """bq clientを返す関数"""
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(get_ssm_parameter(SSM_GCP_KEY_PARAMETER_NAME))
    )
    return bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)


def get_dataset_name(table_name):
    if table_name in get_my_dataset_table_list():
        return DATASET
    else:
        return DATASET_COMMERCE_FLOW


@lru_cache(maxsize=None)
def get_my_dataset_table_list():
    files = glob.glob(os.path.join(MY_TABLE_SCHEMA_LIST_DIR, "*.json"))

    return [os.path.splitext(os.path.basename(x))[0] for x in files]


def get_schema_client():
    """schema取得用のbq clientを返す関数"""
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(get_ssm_parameter(SSM_GCP_KEY_PARAMETER_NAME_SCHEMA))
    )
    return bigquery.Client(
        project=GCP_PROJECT_ID_SCHEMA, credentials=credentials
    )


def get_schema_by_json(table_name):
    path = os.path.join(
        MY_TABLE_SCHEMA_LIST_DIR,
        f"{table_name}.json"
    )
    with open(path, "r") as f:
        schema = json.load(f)
    schema = [
        bigquery.schema.SchemaField(
            s["name"],
            s["type"],
            mode=s["mode"],
            description=s["description"]
        )
        for s in schema
    ]
    return schema


def get_schema_by_table_name(table_name):
    """テーブル名からスキーマを取得する関数"""
    client = get_client()
    table_id = "{}.{}.{}".format(client.project, get_dataset_name(table_name), table_name)
    table = client.get_table(table_id)

    return table.schema


def convert_dtypes(df, schema):
    """dfをスキーマに合わせる関数"""
    for s in schema:
        if s.field_type in {"TIMESTAMP"}:
            df[s.name] = pd.to_datetime(df[s.name].astype(str))
            df[s.name] = df[s.name].apply(
                lambda x: x.strftime("%Y-%m-%d %H:%M")
            )
        elif s.field_type in {"DATE"}:
            df[s.name] = pd.to_datetime(df[s.name].astype(str))
            df[s.name] = df[s.name].apply(lambda x: x.strftime("%Y-%m-%d"))
        elif s.field_type in {"INTEGER", "INT64"}:
            df[s.name] = df[s.name].astype("Int64")
        elif s.field_type in {"NUMERIC"}:
            df[s.name] = df[s.name].astype(float).apply(lambda x: f"{x:.9f}")
        elif s.field_type in {"BOOL", "BOOLEAN"}:
            df[s.name] = df[s.name].astype('boolean')

    return df


def create_df(table_name, data_list):
    """dataclassのリストからdfを作成する関数"""
    schema = get_schema_by_table_name(table_name)

    # initialize df
    df = pd.DataFrame(columns=[s.name for s in schema])
    for datum in data_list:
        df = df.append(dc.asdict(datum), ignore_index=True)
    return df, table_name


def _insert_df(df, table_name):
    """dfをbqにインサートする関数"""
    client = get_client()
    table_id = "{}.{}.{}".format(client.project, get_dataset_name(table_name), table_name)
    table = bigquery.Table(table_id)

    schema = get_schema_by_table_name(table_name)
    df = convert_dtypes(df, schema)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        schema=schema,
        ignore_unknown_values=True,
        autodetect=False,
        write_disposition="WRITE_TRUNCATE",
    )

    if len(df) == 0:
        return
    cols = [x.name for x in schema]
    logging.info(schema)

    df = df[cols]
    logging.info(df)
    logging.info(f"inserting df to {table_name}")

    job = client.load_table_from_dataframe(
        df, table, job_config=job_config
    )
    job.result()


def _load_recursive_default_testcases(data, default={}):
    ret = copy.deepcopy(default)
    current_default = data["default"] if "default" in data.keys() else {}

    for k, v in data.items():
        if k == "default":
            continue

        if isinstance(v, list):
            ret[k] = [_load_recursive_default_testcases(tmp, current_default) for tmp in v]
        elif isinstance(v, dict):
            tmp = _load_recursive_default_testcases(v, current_default)
            if k in ret.keys():
                ret[k].update(tmp)
            else:
                ret[k] = tmp
        else:
            ret[k] = v

    return ret


def _load_recursive_testcases(data, default):
    ret = copy.deepcopy(default)

    for k, v in data.items():
        if isinstance(v, list):
            ret[k] = [_load_recursive_testcases(tmp, default[k][i]) for i, tmp in enumerate(v)]
        elif isinstance(v, dict):
            tmp = _load_recursive_testcases(v, default[k])
            if k in ret.keys():
                ret[k].update(tmp)
            else:
                ret[k] = tmp
        else:
            ret[k] = v

    return ret


def _recursive_id_unique(data, testcase_id):
    if isinstance(data, dict):
        if "id" in data.keys():
            data["id"] = int(f"{testcase_id}{data['id']}")

        if "campaign_id" in data.keys():
            data["campaign_id"] = int(f"{testcase_id}{data['campaign_id']}")

        for k, v in data.items():
            data[k] = _recursive_id_unique(v, testcase_id)

    if isinstance(data, list):
        data = [_recursive_id_unique(v, testcase_id) for v in data]

    return data


def load_test_cases(path="./testcase.yml"):
    with open(path, "r") as f:
        testcase_raw = yaml.safe_load(f)

    default = _load_recursive_default_testcases(testcase_raw["default"])
    cases = list()
    for i, case in enumerate(testcase_raw["cases"]):
        testcase_id = i + 1
        cases.append({
            "id": testcase_id,
            "advertising_account_id": testcase_id,
            "portfolio_id": testcase_id ** 2,
            **_recursive_id_unique(_load_recursive_testcases(case, default), testcase_id)
        })

    return cases


def create_dfs():
    """テストデータを生成する関数"""
    set_seed()
    df_table_list = list()
    cases = load_test_cases()

    converters = list()
    for case in cases:
        testcase = TestCase(**case)

        converter = Converter(testcase)
        converters.append(converter)

    for table_name, method_name in [
        ("advertising_accounts", "AdvertisingAccount"),
        ("portfolios", "Portfolio"),
        ("campaigns", "Campaign"),
        ("ad_groups", "AdGroup"),
        ("keywords", "Keyword"),
        ("keyword_queries", "KeywordQuery"),
        ("product_targetings", "ProductTargeting"),
        ("targetings", "Targeting"),
        ("daily_keywords", "DailyKeywordList"),
        ("daily_keyword_queries", "DailyKeywordQueryList"),
        ("daily_product_targetings", "DailyProductTargetingList"),
        ("daily_targetings", "DailyTargetingList"),
        ("daily_ad_groups", "DailyAdGroupList"),
        ("daily_campaigns", "DailyCampaignList"),
        ("optimization_ad_settings", "OptimizationAdSetting"),
        ("optimization_settings", "OptimizationSetting"),
        ("bidding_unit_info", "BiddingUnitInfo"),
        ("bidding_ad_performance", "BiddingAdPerformanceList"),
        ("ad_group_histories", "AdGroupHistoryList"),
        ("keyword_histories", "KeywordHistoryList"),
        ("product_targeting_histories", "ProductTargetingHistoryList"),
        ("targeting_histories", "TargetingHistoryList"),
        ("ml_result_unit", "MLResultUnit"),
        ("ml_result_campaign", "MLResultCampaign"),
        ("ml_result_ad", "MLResultAd"),
    ]:
        data_list = list()
        for converter in converters:
            data = getattr(converter, f"to{method_name}")()
            if data is None:
                continue
            elif not isinstance(data, list):
                data = [data]
            data_list.extend(data)

        df, table_name = create_df(table_name, data_list)
        df_table_list.append((df, table_name))
    return df_table_list


def _reset_dataset(dataset):
    """
    データセットを一度削除して再度作成する関数
    """
    client = get_client()
    dataset_id = "{}.{}".format(client.project, dataset)

    client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "asia-northeast1"

    client.create_dataset(dataset)


def _delete_dataset(dataset):
    """データセットを削除する関数"""
    client = get_client()
    dataset_id = "{}.{}".format(client.project, dataset)
    client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@invoke.task
def delete_dataset(c):
    """データセットを削除するコマンド"""
    _delete_dataset(DATASET)
    _delete_dataset(DATASET_COMMERCE_FLOW)


@invoke.task
def reset_dataset(c):
    """
    データセットを一度削除して再度作成するコマンド
    """
    _reset_dataset(DATASET)
    _reset_dataset(DATASET_COMMERCE_FLOW)


def _create_all_tables(source_dataset, destination_dataset, by_json=False):
    """
    指定したプロジェクトのデータセットからテーブルを作成するコマンド
    """
    schema_client = get_schema_client()
    client = get_client()
    # データセット内のテーブル情報を取得
    dataset_id = f"{GCP_PROJECT_ID_SCHEMA}.{source_dataset}"
    dataset = schema_client.get_dataset(dataset_id)
    tables = schema_client.list_tables(dataset)

    if not tables:
        raise ValueError("dataset for schema does not contain any tables.")

    # 年月日のサフィックスがついたテーブルはログテーブルなのでスキップする
    skip_suffix = r'^.+\_\d{8}$'

    # 各テーブルのスキーマとパーティション情報を元にテスト用のプロジェクトにテーブルを作成
    for obj in tables:
        table_name = obj.reference.table_id

        if re.match(skip_suffix, str(table_name)):
            continue

        schema_table = schema_client.get_table(obj.reference)
        schema = schema_table.schema
        if by_json:
            # jsonのschema定義がある場合には，そちらを用いる
            try:
                schema = get_schema_by_json(table_name)
            except FileNotFoundError:
                logging.exception(f"Could not load schema definition of {table_name}")

        partition = schema_table.time_partitioning

        table_id = "{}.{}.{}".format(
            client.project, destination_dataset, table_name)
        table = bigquery.Table(table_id, schema=schema)
        if partition:
            table.time_partitioning = partition

        client.create_table(table)


@invoke.task
def create_all_tables(c):
    _create_all_tables(DATASET_NAME_SCHEMA, DATASET, by_json=True)
    _create_all_tables(COMMERCE_FLOW_DATASET_NAME_SCHEMA, DATASET_COMMERCE_FLOW)


@invoke.task
def prepare_test_data(c):
    """
    データを生成してBQにインサートするコマンド
    """
    df_table_list = create_dfs()
    with TPE(max_workers=20) as exe:
        futures = {}
        for df, table_name in df_table_list:
            futures[table_name] = exe.submit(_insert_df, df, table_name)
        wait(futures.values())
        for table_name, future in futures.items():
            logging.info(f"check result: {table_name}")
            _ = future.result()


@invoke.task
def prepare_target_unit(c):
    """
    ML適用対象ユニットを指定するCSVを作成し、S3にアップロードするコマンド
    """
    df = get_df_from_bq_table("bidding_unit_info")

    df = df.groupby(["advertising_account_id", "portfolio_id"], dropna=False).sum()
    df = df.reset_index()
    df = df[["advertising_account_id", "portfolio_id"]]
    df["portfolio_id"] = df["portfolio_id"].astype("Int64")
    s3_uri = get_ssm_parameter(SSM_TARGET_UNIT_PARAMETER_NAME)
    print(f"s3_uri: {s3_uri}, df_length: {len(df)}")
    write_df_to_s3(df, s3_uri)


def _s3_file_key(advertising_account_id, portfolio_id, dt=None, prefix=None):
    today = dt if dt is not None else datetime.datetime.now()

    day_prefix = today.strftime("%Y/%m/%d/%Y%m%d")
    stage = os.environ["OUTPUT_STAGE"]

    if portfolio_id is None or isinstance(portfolio_id, pd._libs.missing.NAType):
        key = f"{day_prefix}_adAccount_" + \
            f"{advertising_account_id}_{stage}.json"
    else:
        key = f"{day_prefix}_portfolio_" + \
            f"{advertising_account_id}_{portfolio_id}_{stage}.json"

    if prefix:
        return f"{prefix}/{key}"
    else:
        return key


def parallel_run(commdand_list):
    promises = list()
    for cmd in commdand_list:
        p = invoke.run(cmd, asynchronous=True)
        promises.append(p)
    for p in promises:
        p.join()


@invoke.task
def build_container(c, name, path):
    """コンテナをビルドするコマンド
    Args:
        name: ビルドしたコンテナにつける名前
        path: Dockerfileのあるディレクトリへのパス
    """
    time.sleep(5 * random.random())

    def build_cmd(work_base_dir, work_app_dir, tag):
        cmd = (
            "DOCKER_BUILDKIT=1 "
            f"docker build -f {work_app_dir}/Dockerfile {work_base_dir} "
            f"-t {name}:{tag} "
            "--build-arg BUILDKIT_INLINE_CACHE=1 --build-arg NO_DEV=yes"
        )
        return cmd

    with tempfile.TemporaryDirectory() as work_base_dir:
        work_app_dir = os.path.join(work_base_dir, name)
        invoke.run(f"cp -R {path} {work_app_dir}")
        invoke.run(f"cp -R ../common_module {work_app_dir}/common_module")
        invoke.run(f"cp -R ../schema {work_app_dir}/schema")
        invoke.run(f"cp -R ../sophia-ai/spai {work_app_dir}/spai")
        invoke.run(f"cp -R ../pyproject {work_base_dir}/pyproject")
        # 最終イメージのビルド
        invoke.run(build_cmd(work_base_dir, work_app_dir, "latest"))


@invoke.task
def build_container_by_name(c, name):
    """コンテナをビルドするコマンド
    Args:
        name: ビルドするディレクトリ
    """
    path = f"../{name}"
    invoke.run(f"inv build-container {name} {path}")


@invoke.task
def prepare_containers(c):
    """コンテナたちをビルドしてディレクトリ名のタグをつけるコマンド"""
    cmds = list()
    for dockerfile in glob.glob("../**/Dockerfile"):
        name = dockerfile.split("/")[-2]
        path = dockerfile.replace("Dockerfile", "")
        if name in BUILD_TARGETS:
            cmds.append(f"inv build-container {name} {path}")
    parallel_run(cmds)


@invoke.task
def run_batch(c, name, command):
    logging.info(f"Running Batch [{name}] with [{command}]")
    """Batchを実行するコマンド
    Args:
        name: コンテナの名前
        command: Batchに与えるコマンド
    """
    contariner_name = f"{uuid.uuid4()}"
    network_option = (
        f" --net={os.environ['NETWORK_NAME']} "
        if "NETWORK_NAME" in os.environ.keys() else ""
    )
    r = invoke.run(
        f"docker run --rm --env-file .env {network_option} {_minio_docker_env_options()}"
        f" --name {contariner_name} {name} {command}", warn=True
    )
    logging.info(f"Running Batch Result: return_code={r.return_code}")
    if r.return_code != 0:
        raise Exception(f"Error Occured in {name}")


def _run_lambda(name, cmd, events):
    logging.info(f"Running Lambda [{name}] with [{events}]")
    contariner_name = f"{uuid.uuid4()}"
    network_option = (
        f" --net={os.environ['NETWORK_NAME']} "
        if "NETWORK_NAME" in os.environ.keys() else ""
    )
    port = get_free_tcp_port()
    invoke.run(
        f"docker run --rm --env-file .env -d {network_option} {_minio_docker_env_options()}"
        f" --name {contariner_name} -p {port}:8080 {name} {cmd}"
    )
    time.sleep(5)

    if network_option == "":
        url = f"http://0.0.0.0:{port}/2015-03-31/functions/function/invocations"
    else:
        url = f"http://{contariner_name}:8080/2015-03-31/functions/function/invocations"

    headers = {"Content-Type": "application/json"}
    for key, event in events.items():
        logging.info(f"exec {name} - {key}")
        if not isinstance(event, str):
            event = json.dumps(event)
        response = requests.post(url, event, headers=headers).json()
        if isinstance(response, dict) and "errorMessage" in response:
            raise Exception(f"{response}")

    invoke.run(f"docker stop {contariner_name}")

    time.sleep(3)


@invoke.task
def run_main(c):
    """Mainを実行するコマンド
    Args:
        date: 実行日
    """
    name = "main"
    cmd = "lambda_handler.lambda_handler"
    cases = load_test_cases()
    events = {}
    for case in cases:
        testcase = TestCase(**case)
        event = {
            "date": testcase.processing_date.strftime("%Y-%m-%d"),
            "advertising_account_id": testcase.advertising_account_id,
            "portfolio_id": testcase.portfolio_id,
        }
        if testcase.is_mock_kpi_prediction:
            event["it_mock_kpi_predictions"] = [{
                "ad_type": ad.ad_type,
                "ad_id": ad.id,
                "cpc": ad.today_predicted_cpc,
                "cvr": ad.today_predicted_cvr,
                "spa": ad.today_predicted_spa,
            } for ad in testcase.ad.list]

        events[f"{testcase.advertising_account_id}-{testcase.portfolio_id}"] = event

    _run_lambda(name, cmd, events)


@invoke.task
def run_record_to_bq(c):
    """record_to_bqを実行するコマンド
    Args:
        date: 実行日
    """
    name = "record_to_bq"
    cmd = "lambda_handler.lambda_handler"

    cases = load_test_cases()
    events = {}
    for case in cases:
        testcase = TestCase(**case)
        date = testcase.processing_date
        if date in events.keys():
            continue

        events[f"{date}"] = {
            "date": date.strftime("%Y-%m-%d"),
        }

    _run_lambda(name, cmd, events)


def get_output_json_data_from_s3(bucket, key):
    s3 = _s3_resource()
    try:
        content_object = s3.Object(bucket, key)
        file_content = content_object.get()["Body"].read().decode("utf-8")
    except s3.meta.client.exceptions.NoSuchKey:
        return None, None
    else:
        d = json.loads(file_content)

    ret_campaigns = []
    ret_ads = []

    if d["result_type"] == 1:
        for campaign in d["unit"]["campaigns"]:
            ret_campaigns.append({
                "campaign_id": campaign["campaign_id"],
                "daily_budget": campaign["daily_budget"],
            })
            if campaign["ads"] is not None:
                for ad in campaign["ads"]:
                    ret_ads.append({
                        "ad_id": ad["ad_id"],
                        "is_paused": ad["is_paused"],
                        "bidding_price": ad["bidding_price"],
                    })

    return ret_campaigns, ret_ads


def _check_output__bid_direction(testcase, output_ads):
    if len(output_ads) == 0:
        logging.error(
            f"TestCase(id={testcase.id}, name={testcase.name}, "
            f"description={testcase.description}): "
            "bid is not changed."
        )
        return True
    else:
        for output_ad in output_ads:
            testcase_ad = None
            for x in testcase.ad.list:
                if output_ad["ad_id"] == f"{x.ad_type}_{x.id}":
                    testcase_ad = x
                    break

            if testcase_ad is None:
                logging.error(
                    f"TestCase(id={testcase.id}, name={testcase.name}, "
                    f"description={testcase.description}): "
                    "ad is not match."
                )
                return True

            if (
                testcase.validations.bid_direction * (
                    output_ad["bidding_price"] - float(testcase_ad.current_bidding_price)
                ) <= 0
            ):
                logging.error(
                    f"TestCase(id={testcase.id}, name={testcase.name}, "
                    f"description={testcase.description}): "
                    "bidding_price was changed in the wrong direction."
                )
                return True

    return False


def _check_output__budget_direction(testcase, output_campaigns):
    if len(output_campaigns) == 0:
        logging.error(
            f"TestCase(id={testcase.id}, name={testcase.name}, "
            f"description={testcase.description}): "
            "bid is not changed."
        )
        return True
    else:
        for output_campaign in output_campaigns:
            testcase_campaign = None
            for x in testcase.campaign.list:
                if str(output_campaign["campaign_id"]) == str(x.id):
                    testcase_campaign = x
                    break

            if testcase_campaign is None:
                logging.error(
                    f"TestCase(id={testcase.id}, name={testcase.name}, "
                    f"description={testcase.description}): "
                    "campaign is not match."
                )
                return True

            if (
                testcase.validations.budget_direction * (
                    output_campaign["daily_budget"] - float(testcase_campaign.current_daily_budget)
                ) <= 0
            ):
                logging.error(
                    f"TestCase(id={testcase.id}, name={testcase.name}, "
                    f"description={testcase.description}): "
                    "daily_budget was changed in the wrong direction."
                )
                return True

    return False


@invoke.task
def check_output(c):
    """ML_OUTPUT_BUCKETの結果を確認するコマンド。
    Args:
        date: 実行日
    """
    print("start check_output")
    bucket = os.getenv("OUTPUT_JSON_BUCKET")
    json_prefix = os.getenv("OUTPUT_JSON_PREFIX")

    cases = load_test_cases()

    has_error = False
    for case in cases:
        print(case)
        testcase = TestCase(**case)

        key = _s3_file_key(
            testcase.advertising_account_id,
            testcase.portfolio_id,
            testcase.processing_date,
            json_prefix
        )
        output_campaigns, output_ads = get_output_json_data_from_s3(bucket, key)
        if output_campaigns is None and output_ads is None:
            logging.error(
                f"TestCase(id={testcase.id}, name={testcase.name}, "
                f"description={testcase.description}): "
                "output json is not exported to the bucket."
            )
            has_error = True
        else:
            if testcase.validations.bid_direction is not None:
                has_error = has_error or _check_output__bid_direction(testcase, output_ads)

            if testcase.validations.budget_direction is not None:
                has_error = has_error or _check_output__budget_direction(testcase, output_campaigns)

    if has_error:
        sys.exit(1)


@lru_cache(maxsize=None)
def get_df_from_bq_table(table):
    client = get_client()
    sql = f"SELECT * FROM `{get_dataset_name(table)}.{table}`;"

    df = client.query(sql).to_dataframe()

    if "portfolio_id" in df.columns:
        df["portfolio_id"] = df["portfolio_id"].astype("Int64")

    return df
