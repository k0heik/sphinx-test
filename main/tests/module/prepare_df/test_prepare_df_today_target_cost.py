import datetime

import pandas as pd
from module import prepare_df


def test_prepare_df_today_target_cost(
    daily_budget_boost_coefficient_df,
):
    today = datetime.datetime(2022, 1, 20)
    optimization_costs = 10000
    latest_cost_df = pd.DataFrame({
        "date": today - datetime.timedelta(days=1),
        "unit_cumsum_costs": [5000],
    })

    daily_budget_boost_coefficient_df["start_date"] = today
    daily_budget_boost_coefficient_df["end_date"] = today
    daily_budget_boost_coefficient_df["coefficient"] = 2.0

    result_df = prepare_df._today_target_cost(
        today, optimization_costs, latest_cost_df, daily_budget_boost_coefficient_df)

    assert len(result_df) == 1
    assert result_df["ideal_target_cost"].values[0] == 2 * 10000 / 32
    assert result_df["allocation_target_cost"].values[0] == 2 * 5000 / 13
    assert "target_cost" in result_df.columns
