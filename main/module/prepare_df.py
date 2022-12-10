import datetime
import itertools
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np

from spai.utils.kpi import (
    target_kpi,
    target_kpi_value,
    adjust_roas_target,
    purpose,
    mode,
    remaining_days,
    target_cost,
)

from common_module.extract_util import complement_daily_ad

from module import config, args


_KPI_C_COLUMNS_MAP = {
    "CPC": ("costs", "clicks"),
    "CPA": ("costs", "conversions"),
    # ROASは本来sales/costsだが、Cにおいては計算の便宜上逆数をとる
    "ROAS": ("costs", "sales"),
}


def _unit_costs(all_campaign_df):
    unit_costs_df = (
        all_campaign_df[config.UNIT_KEY + config.DATE_KEY + ["costs"]]
        .groupby(config.UNIT_KEY + config.DATE_KEY, dropna=False)
        .sum()
        .reset_index()
        .rename(columns={"costs": "unit_sum_costs"})
    )

    unit_costs_df["unit_cumsum_costs"] = (
        unit_costs_df.groupby(
            config.UNIT_KEY + [pd.Grouper(freq="M", key="date")], dropna=False
        )["unit_sum_costs"]
        .transform(lambda x: x.cumsum())
        .fillna(0.0)
    )

    return unit_costs_df


def _today_target_cost(today, optimization_costs, latest_cost_df, daily_budget_boost_coefficient_df):
    unit_cumsum_costs = (
        latest_cost_df["unit_cumsum_costs"].values[0]
        if latest_cost_df["date"].dt.month.values[0] == today.month else 0
    )
    df = pd.DataFrame({
        "date": [today],
        "optimization_costs": optimization_costs,
        "used_costs": [unit_cumsum_costs],
    })
    df["remaining_days"] = remaining_days(df)
    monthly_coefficient_df = shape_daily_budget_boost_coefficient(
        daily_budget_boost_coefficient_df,
        today + relativedelta(day=1),
        today + relativedelta(months=+1, day=1, days=-1)
    )
    df = target_cost(df, monthly_coefficient_df)
    df = df.drop(columns=["date", "optimization_costs"])

    return df


def _unit_ex_observed_C(target_kpi, today, ad_target_actual_df):
    def _avoid_zerodenom(*denoms):
        for denom in denoms:
            if denom != 0:
                return denom

        return 1

    if today.day == 1 or target_kpi not in _KPI_C_COLUMNS_MAP.keys():
        return None

    columns_for_sum = ["clicks", "costs", "conversions", "sales"]
    columns = _KPI_C_COLUMNS_MAP[target_kpi]

    performance_sum_in_monthly_seq = ad_target_actual_df[
        ad_target_actual_df["date"].dt.month == today.month][columns_for_sum].sum()

    if target_kpi == "CPC":
        return performance_sum_in_monthly_seq[columns[0]] / _avoid_zerodenom(performance_sum_in_monthly_seq[columns[1]])
    elif target_kpi == "CPA":
        return performance_sum_in_monthly_seq[columns[0]] / _avoid_zerodenom(performance_sum_in_monthly_seq[columns[1]])
    elif target_kpi == "ROAS":
        performance_sum_in_28_days_seq = ad_target_actual_df[
            ad_target_actual_df["date"] >= today - datetime.timedelta(days=28)][columns_for_sum].sum()
        mean_spa_in_28_days = (
            performance_sum_in_28_days_seq["sales"] / performance_sum_in_28_days_seq["conversions"]
            if performance_sum_in_28_days_seq["conversions"] != 0 else 0
        )
        return performance_sum_in_monthly_seq[columns[0]] / _avoid_zerodenom(
            performance_sum_in_monthly_seq[columns[1]],
            mean_spa_in_28_days
        )
    else:
        return None


def _observed_C_yesterday_in_month(_df, today, target_kpi, keys, prefix):
    yesterday = today - datetime.timedelta(days=1)

    req_columns = list(set(itertools.chain.from_iterable(_KPI_C_COLUMNS_MAP.values())))

    df = _df.loc[
        (_df["date"].dt.month == today.month) & (_df["date"] == yesterday), :
    ][keys + req_columns].groupby(keys, dropna=False).sum().reset_index()

    if today.day == 1 or target_kpi not in _KPI_C_COLUMNS_MAP.keys():
        df[f"{prefix}_observed_C_yesterday_in_month"] = None
    else:
        num_column, denom_column = _KPI_C_COLUMNS_MAP[target_kpi]
        df[f"{prefix}_observed_C_yesterday_in_month"] = df[num_column] / df[denom_column]

    return df[keys + [f"{prefix}_observed_C_yesterday_in_month"]]


def _weekly_ema_costs(_df, today, keys, prefix):
    yesterday = today - datetime.timedelta(1)

    # 集計単位に合わせて合計
    df = _df[_df["date"] > yesterday - datetime.timedelta(7)].sort_values("date")[
        ["costs", "date"] + keys
    ].groupby(keys + ["date"], dropna=False).sum().reset_index()

    # レコード欠損日分のデータを補完
    date_df = pd.DataFrame({
        "date": pd.date_range(end=yesterday, freq="D", periods=7)
    })
    key_values_df = df[keys].groupby(keys, dropna=False).last().reset_index()
    date_df["_tmp_merge_key"] = 1
    key_values_df["_tmp_merge_key"] = 1
    date_key_df = pd.merge(date_df, key_values_df, how="outer", on="_tmp_merge_key").drop(columns=["_tmp_merge_key"])

    df = pd.merge(date_key_df, df.sort_values("date"), how="left", on=keys + ["date"])
    df["costs"] = df["costs"].fillna(0)

    # 指数移動平均値を算出
    df = df[["costs"] + keys].groupby(
        keys, dropna=False).ewm(alpha=0.2).mean().reset_index().groupby(
        keys, dropna=False).tail(1).reset_index().rename(columns={
            "costs": f"{prefix}_weekly_ema_costs",
        })

    return df[keys + [f"{prefix}_weekly_ema_costs"]]


def add_unit_key(df):
    df["advertising_account_id"] = args.Event.advertising_account_id
    df["portfolio_id"] = args.Event.portfolio_id

    return df.astype(config.UNIT_KEY_DTYPES)


def add_unit_info_setting_columns(unit_info_df):
    unit_info_df["target_kpi"] = target_kpi(unit_info_df)
    unit_info_df["target_kpi_value"] = adjust_roas_target(
        unit_info_df["target_kpi"], target_kpi_value(unit_info_df)
    )
    unit_info_df["C"] = np.where(
        unit_info_df["target_kpi"] == "ROAS",
        1 / unit_info_df["target_kpi_value"],
        unit_info_df["target_kpi_value"]
    )
    unit_info_df["purpose"] = purpose(unit_info_df["target_kpi"])
    unit_info_df["mode"] = mode(unit_info_df)

    return unit_info_df


def shape_daily_budget_boost_coefficient(
    daily_budget_boost_coefficient_df, from_date, to_date
):

    date_df = pd.DataFrame({
        "date": pd.date_range(from_date, to_date)
    })

    date_df["merge_tmp_key"] = 1
    daily_budget_boost_coefficient_df["merge_tmp_key"] = 1

    tmp_df = pd.merge(
        date_df, daily_budget_boost_coefficient_df,
        how="outer", on="merge_tmp_key")
    tmp_df = tmp_df[
        ~(tmp_df["coefficient"].isnull())
        & (tmp_df["date"] >= tmp_df["start_date"])
        & (tmp_df["date"] <= tmp_df["end_date"])
    ]

    df = pd.merge(date_df, tmp_df, how="left", on="date")
    df["coefficient"].fillna(1.0, inplace=True)

    # 同じ日付に複数設定がある場合は直近で更新があったデータを採用
    df = df.sort_values(["date", "updated_at"]).groupby(["date"]).last().reset_index()

    return df[["date", "coefficient"]]


def commons(
    unit_info_df,
    campaign_info_df,
    campaign_all_actual_df,
    ad_info_df,
    ad_target_actual_df,
    daily_budget_boost_coefficient_df,
):
    today = args.Event.today
    yesterday = today - datetime.timedelta(days=1)

    target_kpi = unit_info_df["target_kpi"].values[0]

    ad_df = pd.merge(
        ad_target_actual_df,
        ad_info_df,
        how="inner",
        on=config.AD_KEY,
        suffixes=["", "_current"],
    )

    all_campaign_df = pd.merge(
        add_unit_key(campaign_all_actual_df),
        campaign_info_df,
        how="left",
        on=config.UNIT_KEY + config.CAMPAIGN_KEY,
    )

    # adデータの前日欠損の補完処理
    complement_ad_info = ad_info_df.copy()
    complement_ad_info["bidding_price_current"] = complement_ad_info["bidding_price"]
    lack_columns = list(
        set(ad_df.columns) - set(complement_ad_info) - set(config.DATE_KEY)
    )
    for col in lack_columns:
        complement_ad_info[col] = 0
    ad_df = complement_daily_ad(
        ad_df,
        complement_ad_info,
        yesterday,
        complement_bidding_price_name="bidding_price_current",
    )
    ad_df["date"] = pd.to_datetime(ad_df["date"])

    # unit残予算関連の計算
    all_campaign_df_original_columns = list(all_campaign_df.columns)
    unit_costs_columns = ["unit_sum_costs", "unit_cumsum_costs"]
    all_campaign_df = pd.merge(
        all_campaign_df, unit_info_df, how="left", on=config.UNIT_KEY
    )

    target_unit_df = unit_info_df.copy()
    unit_cost_df = _unit_costs(all_campaign_df)
    target_unit_df = pd.merge(
        target_unit_df, unit_cost_df.sort_values(["date"]).tail(1),
        how="left", on=config.UNIT_KEY)
    target_unit_df = pd.concat([
        target_unit_df,
        _today_target_cost(
            today,
            unit_info_df["optimization_costs"].values[0],
            target_unit_df,
            daily_budget_boost_coefficient_df
        )],
        axis=1
    )
    target_unit_df = pd.merge(
        target_unit_df, _weekly_ema_costs(all_campaign_df, today, config.UNIT_KEY, "unit"),
        how="left", on=config.UNIT_KEY)
    target_unit_df["unit_ex_observed_C"] = _unit_ex_observed_C(target_kpi, today, ad_target_actual_df)

    all_campaign_df = pd.merge(
        all_campaign_df, target_unit_df, how="left", on=config.UNIT_KEY + config.DATE_KEY)

    # ad単位のdf成形
    target_ad_df = pd.merge(
        ad_df,
        all_campaign_df[all_campaign_df_original_columns + unit_costs_columns],
        how="left",
        on=config.UNIT_KEY + config.CAMPAIGN_KEY + config.DATE_KEY,
        suffixes=["", "_campaign"],
    )
    target_ad_df = pd.merge(
        target_ad_df,
        _observed_C_yesterday_in_month(target_ad_df, today, target_kpi, config.AD_KEY, "ad"),
        how="left", on=config.AD_KEY)
    target_ad_df = pd.merge(
        target_ad_df, _weekly_ema_costs(target_ad_df, today, config.AD_KEY, "ad"),
        how="left", on=config.AD_KEY)
    target_ad_df = pd.merge(target_ad_df, unit_info_df, how="left", on=config.UNIT_KEY)

    # campaign単位のdf成形
    target_campaign_ids_df = (
        target_ad_df.groupby(config.CAMPAIGN_KEY, dropna=False)
        .last()
        .reset_index()["campaign_id"]
    )
    target_campaign_df = pd.merge(
        all_campaign_df, target_campaign_ids_df, how="inner", on=config.CAMPAIGN_KEY)
    target_campaign_df = pd.merge(
        target_campaign_df,
        _observed_C_yesterday_in_month(
            target_campaign_df, today, target_kpi, config.CAMPAIGN_KEY, "campaign"),
        how="left", on=config.CAMPAIGN_KEY
    )
    target_campaign_df = pd.merge(
        target_campaign_df, _weekly_ema_costs(target_campaign_df, today, config.CAMPAIGN_KEY, "campaign"),
        how="left", on=config.CAMPAIGN_KEY)

    # 不要な値だがないと各所でエラーが起こるためダミー値設定
    target_ad_df["ad_group_id"] = 0

    return target_ad_df, target_campaign_df, target_unit_df
