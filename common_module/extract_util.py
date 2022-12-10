import pandas as pd

from common_module.logger_util import get_custom_logger


logger = get_custom_logger()

fill_0_columns = [
    "impressions",
    "clicks",
    "costs",
    "conversions",
    "sales"
]


def complement_log(row):
    logger.warning(
        "complement " +
        f"advertising_account_id: {row['advertising_account_id']}," +
        f"portfolio_id: {row['portfolio_id']}," +
        f"campaign_id: {row['campaign_id']}," +
        f"ad_type: {row['ad_type']}," +
        f"ad_id: {row['ad_id']}"
    )


def complement_daily_ad(
        df, ad_df, complement_date, need_bidding_price=True, complement_bidding_price_name="current_bidding_price"):
    """dfの最新日付に含まれていないadのデータをad_dfのデータから補完する

    Args:
        df ([type]): 実績データのDataFrame
        ad_df ([type]): 対象広告のDataFrame
        complement_date: 補完対象の日付
    """
    unique_key_columns = ["ad_type", "ad_id"]
    complement_date = complement_date.strftime('%Y-%m-%d')
    df['date'] = pd.to_datetime(df['date'])
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    keys = [tuple(x) for x in
            df[df['date'] == complement_date][unique_key_columns].values]

    ad_df['keys'] = [tuple(x) for x in ad_df[unique_key_columns].values]

    complement_df = ad_df[~ad_df['keys'].isin(keys)].copy()

    for col in fill_0_columns:
        complement_df[col] = 0

    if need_bidding_price:
        complement_df['bidding_price'] = complement_df[complement_bidding_price_name].values
    complement_df['date'] = complement_date

    complement_df = complement_df[df.columns]

    if len(complement_df) > 0:
        complement_df.apply(complement_log, axis=1)
        df = pd.concat([df, complement_df], axis=0, ignore_index=True)

    return df
