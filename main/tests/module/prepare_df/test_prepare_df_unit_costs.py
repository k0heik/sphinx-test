import datetime
import itertools

import pandas as pd
import pytest

from module import prepare_df, config


@pytest.mark.parametrize("portfolio_id", [1, None])
@pytest.mark.parametrize(
    (
        "end_date, days, campaign_num, daily_costs,"
        "expected_unit_sum_costs, expected_unit_cumsum_costs"
    ),
    [
        (datetime.datetime(2021, 12, 31), 2, 2, 1, 2, 2 * 2),
        (datetime.datetime(2021, 12, 31), 3, 2, 1, 2, 2 * 3),
        (datetime.datetime(2021, 12, 31), 60, 2, 1, 2, 2 * 31),
        (datetime.datetime(2021, 12, 30), 60, 2, 1, 2, 2 * 30),
        (datetime.datetime(2021, 12, 30), 60, 3, 2, 6, 6 * 30),
        (datetime.datetime(2021, 12, 1), 2, 2, 1, 2, 2),
    ],
)
def test_prepare_df_unit_costs(
    portfolio_id,
    end_date,
    days,
    campaign_num,
    daily_costs,
    expected_unit_sum_costs,
    expected_unit_cumsum_costs,
):
    advertising_account_id = 9999

    df = pd.DataFrame(
        {
            "advertising_account_id": [advertising_account_id] * campaign_num * days,
            "portfolio_id": [portfolio_id] * campaign_num * days,
            "date": list(pd.date_range(end=end_date, freq="D", periods=days))
            * campaign_num,
            "campaign_id": list(
                itertools.chain.from_iterable(
                    [[i] * days for i in range(100, 100 + campaign_num)]
                )
            ),
            "costs": [daily_costs] * campaign_num * days,
        }
    ).astype(config.UNIT_KEY_DTYPES)

    result_df = prepare_df._unit_costs(df)

    assert len(result_df) == days

    assert result_df["date"].values[-1] == pd.to_datetime(end_date)
    assert result_df["unit_sum_costs"].values[-1] == expected_unit_sum_costs
    assert result_df["unit_cumsum_costs"].values[-1] == expected_unit_cumsum_costs
