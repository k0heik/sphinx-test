import datetime
import pytest

import pandas as pd

from module import pid_controller


@pytest.mark.parametrize("days", [1, 5, 10, 40])
def test_pid_controller_calc_sum_actual(days):
    today = datetime.datetime(2022, 1, 1)
    dfs = []
    for i in range(1, 1 + 10):
        dfs.append(
            pd.DataFrame(
                {
                    "ad_type": ["ad_type"] * days,
                    "ad_id": [i] * days,
                    "date": pd.date_range(end=today, freq="D", periods=days),
                    "impressions": [1] * days,
                    "clicks": [1] * days,
                    "conversions": [1] * days,
                    "costs": [1] * days,
                    "sales": [1] * days,
                }
            )
        )

    df = pd.concat(dfs).reset_index()

    result_df = pid_controller._calc_sum_actual(df, "name_7D", "7D")
    result_df = pid_controller._calc_sum_actual(result_df, "name_28D", "28D")

    assert len(result_df) == len(df)

    for i in range(1, 1 + 10):
        assert list(
            result_df[result_df["ad_id"] == i]["clicks_name_7D"].values
        ) == list(range(1, 1 + days))[: min(7, days)] + ([7] * max(0, days - 7))
        assert list(
            result_df[result_df["ad_id"] == i]["clicks_name_28D"].values
        ) == list(range(1, 1 + days))[: min(28, days)] + ([28] * max(0, days - 28))
