import os
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
def _get_deletation_target_table_names(credentials, project_id, dataset_name, deletation_target_table_prefixes):
    client = bigquery.Client(
        project=project_id,
        credentials=credentials)

    tables = client.list_tables(f"{project_id}.{dataset_name}")
    table_names = [x.table_id for x in tables]

    return list(filter(lambda name: name.startswith(deletation_target_table_prefixes), table_names))


@_pring_log
def _update_table(table_name, credentials, project_id, dataset):
    client = bigquery.Client(
        project=project_id,
        credentials=credentials)

    sql = f"""UPDATE `{project_id}.{dataset}.{table_name}`
                SET portfolio_id = NULL
                WHERE portfolio_id = -1
"""

    query_job = client.query(sql)
    result = query_job.result()

    print(result)

    return result

@_pring_log
def _delete_table(table_name, credentials, project_id, dataset_name):
    client = bigquery.Client(
        project=project_id,
        credentials=credentials)

    client.delete_table(f"{project_id}.{dataset_name}.{table_name}")


@_pring_log
def main():
    GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
    DATASET = os.environ["DATASET"]
    WORKING_DATASET = os.environ["WORKING_DATASET"]

    credentials = _get_gcp_credentials()

    DELETION_TARGET_TABLE_PREFIXES = (
        "bidding_preprocess_",
        "cap_daily_budget_",
        "ctr_",
        "cvr_",
        "ml_validation_",
        "pid_",
        "spa_",
    )

    UPDATE_TABLE_LIST = [
        "ctr_prediction",
        "cvr_prediction",
        "spa_prediction",
        "pid_result",
        "ml_bid_result",
        "cap_daily_budget_result",
        "bidding_unit_info",
        "bidding_ad_performance",
        "bidding_input",
        "ml_apply",
        "ml_metric",
        "ml_monitoring",
        "ml_validation",
        "ml_validation_details",
    ]

    deletation_target_table_names = _get_deletation_target_table_names(
        credentials, GCP_PROJECT_ID, WORKING_DATASET, DELETION_TARGET_TABLE_PREFIXES)
    print(f"deletation_target_table_names: {deletation_target_table_names}")
    if _is_exec(f"{WORKING_DATASET}のワークテーブル削除を実行しますか？", "yes"):
        for table_name in deletation_target_table_names:
            _delete_table(table_name, credentials, GCP_PROJECT_ID, WORKING_DATASET)

    if _is_exec(f"{DATASET}のrpc_predictionテーブル削除を実行しますか？", "yes"):
        _delete_table("rpc_prediction", credentials, GCP_PROJECT_ID, DATASET)

    if _is_exec(f"{DATASET}のUPDATEを実行しますか？", "yes"):
        for table_name in UPDATE_TABLE_LIST:
            _update_table(table_name, credentials, GCP_PROJECT_ID, DATASET)


if __name__ == "__main__":
    main()
