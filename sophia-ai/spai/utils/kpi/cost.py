import pandas as pd
import numpy as np


def remaining_days(df: pd.DataFrame, date_column: str = "date") -> pd.Series:
    days_in_month = df[date_column].dt.days_in_month
    r_days = days_in_month - df[date_column].dt.day + 1  # include current day
    return r_days


def target_cost(_df: pd.DataFrame, monthly_coefficient_df: pd.DataFrame) -> pd.Series:
    required_columns = [
        'remaining_days',
        'optimization_costs',
        'used_costs'
    ]
    for column in required_columns:
        if column not in _df.columns:
            raise ValueError(f'{column} must be calculated')

    tmp_coefficient_df = monthly_coefficient_df.copy()
    tmp_coefficient_df["used_coefficient"] = tmp_coefficient_df["coefficient"].cumsum().shift()
    tmp_coefficient_df["used_coefficient"].fillna(0.0, inplace=True)
    df = pd.merge(_df, tmp_coefficient_df, how="left", on="date")
    df["total_coefficient"] = monthly_coefficient_df["coefficient"].sum()

    df["remaining_coefficient"] = df["total_coefficient"] - df["used_coefficient"]

    df["ideal_target_cost"] = (
        df['optimization_costs'] *
        df["coefficient"] / df["total_coefficient"]
    )
    df["allocation_target_cost"] = np.maximum(
        0,
        (df['optimization_costs'] - df['used_costs']) *
        df["coefficient"] / df["remaining_coefficient"]
    )

    df["target_cost"] = np.where(
        (df['remaining_days'] > 1)
        & (df['allocation_target_cost'] > df['ideal_target_cost']),
        2 * df["allocation_target_cost"] - df["ideal_target_cost"],
        df["allocation_target_cost"],
    )

    df["noboost_target_cost"] = df["target_cost"] / df["coefficient"]

    return df


def used_cost(df: pd.DataFrame) -> pd.DataFrame:
    unit_pk_cols = ["advertising_account_id", "portfolio_id", "date"]
    unit_df = df.groupby(unit_pk_cols, dropna=False, as_index=False).last()
    unit_df["portfolio_id"] = unit_df["portfolio_id"].astype("Int64")
    unit_df["sum_costs"] = unit_df["sum_costs"].fillna(0.0)

    unit_df["used_costs"] = unit_df.groupby([
        'portfolio_id',
        'advertising_account_id',
        pd.Grouper(freq='M', key='date'),
    ], dropna=False)['sum_costs'].transform(lambda x: x.cumsum().shift()).fillna(0.0)

    return pd.merge(
        df,
        unit_df[unit_pk_cols + ["used_costs"]],
        how='left',
        on=unit_pk_cols,
    )
