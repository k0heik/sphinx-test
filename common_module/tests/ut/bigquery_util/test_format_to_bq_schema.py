import pandas as pd
import random
from common_module.bigquery_util import format_to_bq_schema


def test_format_to_bq_schema(mocker):
    columns = ["A", "B", "C", "D", "E"]
    mocker.patch(
      "common_module.bigquery_util.funcs.load_output_table_schema",
      return_value=[{"name": c} for c in columns])

    df = pd.DataFrame(columns=random.sample(columns, len(columns) - 1))

    assert list(columns) != list(df.columns)
    assert len(columns) != len(df.columns)
    assert list(columns) == list(format_to_bq_schema(df, "dummyname").columns)
