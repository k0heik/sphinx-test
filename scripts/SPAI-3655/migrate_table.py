import os
import time
import json
from google.cloud import bigquery
from google.oauth2 import service_account
from common_module.aws_util import get_ssm_parameter


def _is_exec(msg, accept_str="yes"):
    print(f"{msg} 実行するなら「{accept_str}」と入力してください。：")

    return input() == accept_str


def _pring_log(func):
    def wrapper(*args, **kwargs):
        try:
            print(
                f"Start func=[{func.__name__}] args={args}, kwargs={kwargs}")
            res = func(*args, **kwargs)
            print(
                f"Finished func=[{func.__name__}] args={args}, kwargs={kwargs}")

            return res
        except Exception as e:
            raise e
    return wrapper


def _get_gcp_credentials():
    return service_account.Credentials.from_service_account_info(
        json.loads(
            get_ssm_parameter(
                os.environ["SSM_GCP_KEY_PARAMETER_NAME"])))


@_pring_log
def _create_dataset(credentials, project_id, dataset_name):
    client = bigquery.Client(
        project=project_id,
        credentials=credentials)

    dataset = bigquery.Dataset(f"{project_id}.{dataset_name}")
    dataset.location = "asia-northeast1"
    dataset = client.create_dataset(dataset, timeout=30)


@_pring_log
def _copy_table(table_name, credentials, project_id, original_dataset, destination_dataset):
    client = bigquery.Client(
        project=project_id,
        credentials=credentials)

    job = client.copy_table(
        f"{project_id}.{original_dataset}.{table_name}",
        f"{project_id}.{destination_dataset}.{table_name}"
    )
    job.result()


@_pring_log
def _delete_table(table_name, credentials, project_id, dataset_name):
    client = bigquery.Client(
        project=project_id,
        credentials=credentials)

    client.delete_table(f"{project_id}.{dataset_name}.{table_name}")


@_pring_log
def main():
    ORIGINAL_DATASET = os.environ["ORIGINAL_DATASET"]
    DESTINATION_DATASET = os.environ["DESTINATION_DATASET"]
    GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]

    credentials = _get_gcp_credentials()

    MIGRATION_TABLE_LIST = [
        "ctr_prediction",
        "cvr_prediction",
        "rpc_prediction",
        "pid_result",
        "ml_bid_result",
        "cap_daily_budget_result",
        "bidding_unit_info",
        "bidding_ad_performance",
        "bidding_input",
        "ml_validation",
        "ml_validation_details",
        "ml_metric",
        "ml_apply",
    ]

    DELETE_TABLE_LIST = MIGRATION_TABLE_LIST + [
        "bidding_output_result",
        "bidding_output_unit_info",
        "bidding_output_campaign",
        "bidding_output_ad",
    ]

    if _is_exec(f"データセット{DESTINATION_DATASET}を作成しますか？", "yes"):
        _create_dataset(credentials, GCP_PROJECT_ID, DESTINATION_DATASET)
    else:
        return

    if _is_exec(f"{ORIGINAL_DATASET}から{DESTINATION_DATASET}へのテーブルコピーを実行しますか？", "yes"):
        for table_name in MIGRATION_TABLE_LIST:
            _copy_table(table_name, credentials, GCP_PROJECT_ID, ORIGINAL_DATASET, DESTINATION_DATASET)
    else:
        return

    if _is_exec(f"{ORIGINAL_DATASET}におけるテーブル削除を実行しますか？", "yes"):
        for table_name in DELETE_TABLE_LIST:
            _delete_table(table_name, credentials, GCP_PROJECT_ID, ORIGINAL_DATASET)
    else:
        return


if __name__ == "__main__":
    main()
