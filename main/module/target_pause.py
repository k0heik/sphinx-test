import datetime

import numpy as np
import pandas as pd

from module import config, args


_BORDER_DAYS_30 = 30
_BORDER_DAYS_60 = 60
_OPERATION_DAYS_THRESHOLD = 30
_COST_RATIO_THRESHOLD = 0.01

OUTPUT_COLUMNS = [
    "ad_type",
    "ad_id",
    "match_type",
    "costs_30days",
    "sales_30days",
    "sales_60days",
    "is_target_pause",
]


def exec(unit_info_df, target_ad_df):
    today = args.Event.today
    if today.day != 1 and unit_info_df["start"].values[0] != pd.to_datetime(today):
        return None

    yesterday = today - datetime.timedelta(1)
    past30days = yesterday - datetime.timedelta(_BORDER_DAYS_30)
    past60days = yesterday - datetime.timedelta(_BORDER_DAYS_60)

    df = target_ad_df[target_ad_df["date"] <= yesterday].reset_index()

    past30days_sum_df = (
        df.loc[df["date"] >= past30days, config.AD_KEY + ["costs", "sales"]]
        .groupby(config.AD_KEY, dropna=False)
        .sum()
        .reset_index()
    )

    past60days_sum_df = (
        df.loc[df["date"] >= past60days, config.AD_KEY + ["costs", "sales"]]
        .groupby(config.AD_KEY, dropna=False)
        .sum()
        .reset_index()
    )

    agg_settings = {
        "match_type": "last",
        "date": ["min", "count"],
    }
    df = (
        df[config.AD_KEY + list(agg_settings.keys())]
        .groupby(config.AD_KEY)
        .agg(agg_settings)
        .reset_index()
    )
    df.columns = [
        "_".join(col) if col[1] != "" else col[0]
        for col in df.columns.to_flat_index().values
    ]
    df = df.rename(columns={"match_type_last": "match_type"})

    df["assume_operation_days"] = (today - df["date_min"]) / datetime.timedelta(days=1)
    df["assume_operation_days"] = np.where(
        df["assume_operation_days"] < df["date_count"],
        df["date_count"],
        df["assume_operation_days"],
    )

    merged_df = pd.merge(
        past30days_sum_df,
        past60days_sum_df,
        how="left",
        on=config.AD_KEY,
        suffixes=["_30days", "_60days"],
    )
    df = pd.merge(df, merged_df, how="left", on=config.AD_KEY)

    optimization_costs = unit_info_df["optimization_costs"].values[0]
    df["is_target_pause"] = False
    df["is_target_pause"] = np.where(
        ((df["ad_type"] == "product_targeting") | (df["match_type"] == "exact")),
        np.where(
            df["assume_operation_days"] >= _OPERATION_DAYS_THRESHOLD,
            (
                ((df["costs_30days"] / optimization_costs) >= _COST_RATIO_THRESHOLD)
                & (df["sales_30days"] == 0)
            ),
            False,
        ),
        df["is_target_pause"],
    )
    df["is_target_pause"] = np.where(
        df["match_type"].isin(["broad", "phrase"]),
        np.where(
            df["assume_operation_days"] >= _OPERATION_DAYS_THRESHOLD,
            (
                ((df["costs_30days"] / optimization_costs) >= _COST_RATIO_THRESHOLD)
                & (df["sales_60days"] == 0)
            ),
            False,
        ),
        df["is_target_pause"],
    )

    return df[OUTPUT_COLUMNS]
