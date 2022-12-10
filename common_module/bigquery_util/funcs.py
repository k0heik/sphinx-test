import os
import json


def load_output_table_schema(table_name):
    path = os.path.join(
        os.environ.get("SCHEMA_DIR_PARENT", ""),
        f"schema/definition/{table_name}.json"
    )
    with open(path) as f:
        return json.loads(f.read())


def format_to_bq_schema(_df, table_name):
    df = _df.copy()
    schema_columns = [f["name"] for f in load_output_table_schema(table_name)]

    for column in list(set(schema_columns) - set(df.columns)):
        df[column] = None

    return df.reindex(columns=schema_columns)[schema_columns]
