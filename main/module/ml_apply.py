import datetime
import numpy as np
import pandas as pd

from module import config, args


_APPLIED_THRESHOLD = 0.5


def exec(ad_target_actual_df, lastday_ml_result_ad_df):
    today = args.Event.today
    if len(lastday_ml_result_ad_df) == 0:
        return False

    yesterday = today - datetime.timedelta(days=1)
    df = ad_target_actual_df[ad_target_actual_df["date"] == yesterday]

    df = pd.merge(
        df,
        lastday_ml_result_ad_df,
        how="left",
        on=config.AD_KEY,
        suffixes=["_actual", "_result"],
    )
    df["bidding_price_result"] = df["bidding_price_result"].fillna(np.nan)

    match_df = df["bidding_price_actual"] == df["bidding_price_result"]

    return match_df.sum() / len(match_df) > _APPLIED_THRESHOLD
