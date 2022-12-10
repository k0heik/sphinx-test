import json
import invoke
from google.cloud import bigquery


SCHEMA_DIR = "./definition"


@invoke.task
def create_dataset(c, dataset_name):
    """データセットを作成するコマンド

    Args:
        c (Context): Context
        dataset_name (str): データセット名
    """
    client = bigquery.Client()
    dataset_id = "{}.{}".format(client.project, dataset_name)

    dataset = bigquery.Dataset(dataset_id)

    dataset.location = "asia-northeast1"

    client.create_dataset(dataset)


@invoke.task
def create_table(c, dataset_name, table_name, partition_field=None):
    """テーブルを作成するコマンド

    Args:
        c (Context): Context
        dataset_name (str): テーブルを作成するデータセット名
        table_name (str): テーブル名
        partition_field (str, optional):
            パーティションキーに指定するフィールド、Noneの場合はパーティションを行わない. Defaults to None.
    """
    client = bigquery.Client()
    table_id = "{}.{}.{}".format(client.project, dataset_name, table_name)
    schema_file = f"{SCHEMA_DIR}/{table_name}.json"
    with open(schema_file) as f:
        schema = json.load(f)

    table = bigquery.Table(table_id, schema=schema)

    if partition_field is not None:
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field
        )

    table = client.create_table(table)
    print(f"Success Create Table: {table_id}")


@invoke.task
def update_table_schema(c, dataset_name, table_name):
    """テーブルのスキーマを更新するコマンド

    Args:
        c (Context): Context
        dataset_name (str): データセット名
        table_name (str): テーブル名
    """
    client = bigquery.Client()
    table_id = "{}.{}.{}".format(client.project, dataset_name, table_name)

    table = client.get_table(table_id)

    original_schema = table.schema

    schema_file = f"{SCHEMA_DIR}/{table_name}.json"
    with open(schema_file) as f:
        new_schema = json.load(f)

    table.schema = new_schema
    table = client.update_table(table, ["schema"])

    if len(original_schema) < len(new_schema):
        add_column_num = len(new_schema) - len(original_schema)
        print(f"added {add_column_num} columns.")
    else:
        print("schema did not change.")


@invoke.task
def delete_table(c, dataset_name, table_name):
    """テーブルを削除するコマンド

    Args:
        c (Context): Context
        dataset_name (str): テーブルを作成するデータセット名
        table_name (str): テーブル名
    """
    client = bigquery.Client()
    table_id = "{}.{}.{}".format(client.project, dataset_name, table_name)

    client.delete_table(table_id, not_found_ok=True)

    print(f"Deleted Table: {table_id}")
