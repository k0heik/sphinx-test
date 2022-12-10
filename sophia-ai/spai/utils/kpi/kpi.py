from datetime import timedelta
import numpy as np
import pandas as pd


agg_feature_cols = ['impressions', 'clicks',
                    'costs', 'conversions', 'sales']

OPTIMIZATION_PURPOSE_TO_KPI = {
    0: 'ROAS', 1: 'CPA', 2: 'CPC'
}

NULL_PURPOSE_DEFAULT = 'SALES'

OPTIMIZATION_PURPOSE_TO_PURPOSE = {
    0: 'SALES', 1: 'CONVERSION', 2: 'CLICK'
}

MODE_BUDGET = "BUDGET"
MODE_KPI = "KPI"


def safe_div(x, y):
    x = np.nan_to_num(x.astype(np.float64), posinf=0, neginf=0)
    y = np.nan_to_num(y.astype(np.float64), posinf=0, neginf=0)
    return np.divide(x, y, out=np.zeros_like(x), where=y != 0)


def feat_column_name(key_column, prefix, agg_field):
    return f'{key_column}_{prefix}_{agg_field}'


def calc_kpis(df: pd.DataFrame) -> pd.DataFrame:
    df['ctr'] = safe_div(df['clicks'].values, df['impressions'].values)
    df['cvr'] = safe_div(df['conversions'].values, df['clicks'].values)
    df['rpc'] = safe_div(df['sales'].values, df['clicks'].values)
    df['spa'] = safe_div(df['sales'].values, df['conversions'].values)
    df['cpc'] = safe_div(df['costs'].values, df['clicks'].values)
    return df


def agg_feats(df, freq, method, prefix, key_column, feature_columns, date_column='date'):
    min_periods = 1

    df = df.sort_values([key_column, date_column])
    res = df[agg_feature_cols + [key_column, date_column]]

    if key_column != "ad_id":
        res = res.fillna(0.0).groupby([key_column, date_column]).mean().reset_index()

    res = res.sort_values([key_column, date_column])
    dates = res[date_column].values
    ids = res[key_column].values
    res = res.fillna(0.0).groupby(key_column, group_keys=False)

    if method == 'rolling':
        res = res.rolling(
            freq,
            on=date_column,
            min_periods=min_periods
        ).mean()[agg_feature_cols]
    else:
        res = ewm(res, freq, min_periods, agg_feature_cols)

    res = res.fillna(0.0).reset_index().rename(columns={'level_1': 'index'})

    # ewmの場合にdateとad_idがindexに含まれない
    if date_column not in res.columns:
        res[date_column] = dates
    if key_column not in res.columns:
        res[key_column] = ids

    added_column = []
    for kpi_name, div_fields in {
        "ctr": ("clicks", "impressions"),
        "cvr": ("conversions", "clicks"),
        "rpc": ("sales", "clicks"),
        "spa": ("sales", "conversions"),
    }.items():
        column = feat_column_name(key_column, prefix, kpi_name)
        if key_column == "ad_id" \
                or any([feature_column.endswith(column) for feature_column in feature_columns]):
            added_column.append(column)
            res[column] = safe_div(
                res[div_fields[0]].values,
                res[div_fields[1]].values
            )

    res = res[[key_column, date_column] + agg_feature_cols + added_column]

    return res


def merge_feats(df, feats, prefix, key_column, date_column='date'):
    df[date_column] = pd.to_datetime(df[date_column])
    feats[date_column] = pd.to_datetime(feats[date_column])
    feats[key_column] = feats[key_column].astype(df[key_column].dtype)
    df = df.sort_values([date_column, key_column])
    feats = feats.sort_values([date_column, key_column])
    out = pd.merge_asof(
        df,
        feats,
        on=date_column,
        by=key_column,
        allow_exact_matches=False,
        suffixes=(
            '',
            f'_{key_column}_{prefix}'),
        direction='backward')
    out.columns = [
        f'{key_column}_{prefix}_{col.replace(f"_{key_column}_{prefix}", "")}'
        if f'_{key_column}_{prefix}' in col else col for col in out.columns]
    return out


def merge_agg_feats(df, freq, method, prefix, key_column, feature_columns=None, date_column='date'):
    df['index'] = df.index
    feats = agg_feats(df, freq, method, prefix, key_column, feature_columns, date_column)
    df = merge_feats(df, feats, prefix, key_column, date_column)
    df = df.sort_values('index').reset_index(drop=True)
    return df.drop('index', axis=1)


def merge_cutoff_feats(df):
    """足切り用の特徴量の計算
    """
    df['index'] = df.index
    df = df.sort_values(['ad_id', 'date'])
    res = df[["ad_id", "date", "clicks", "conversions", "sales"]]
    res = res.fillna(0.0).groupby('ad_id')

    weekly_sum = res.rolling('7D', on='date', min_periods=1)
    monthly_sum = res.rolling('28D', on='date', min_periods=1)

    weekly_sum = weekly_sum[['clicks']].sum().fillna(
        0.0).reset_index().rename(columns={'level_1': 'index'})

    monthly_sum = monthly_sum[['conversions', 'sales']].sum().fillna(
        0.0).reset_index().rename(columns={'level_1': 'index'})

    df = merge_feats(df, weekly_sum, prefix="weekly_sum", key_column="ad_id")
    df = merge_feats(df, monthly_sum, prefix="monthly_sum", key_column="ad_id")

    df = df.sort_values('index').reset_index(drop=True)

    return df.drop('index', axis=1)


def calc_lag(df, column, days=14, key_columns=['ad_type', 'ad_id'], date_column="date", col_name_format="ad_id"):
    df = df.sort_values(key_columns + [date_column])
    for _n in range(days):
        n = _n + 1
        col_name = col_name_format.format(day=n)
        df[col_name] = df[key_columns + [column]].groupby(key_columns, dropna=False)[column].shift(n)

    return df


def calc_mean(df, columns, key_columns=['ad_type', 'ad_id'], prefix="ad_id", date_column="date"):
    group_key = key_columns + [date_column]
    sum_df = df[group_key + columns].groupby(group_key, dropna=False).mean()
    sum_df = sum_df.rename(columns={column: f"{prefix}_{column}" for column in columns})

    return pd.merge(df, sum_df, how="left", on=key_columns + [date_column])


def calc_placement_kpi(df, placement_df, key_columns=['campaign_id'], date_column="date"):
    require_columns = ["placementProductPage_cpc", "placementTop_cpc"]

    if len(placement_df) == 0:
        for column in require_columns:
            df[column] = None
            df[column] = df[column].astype(float)

        return df

    mean_columns = ["costs", "clicks"]
    feat_columns = ["cpc"]

    tmp_df = placement_df.groupby(
        key_columns + [date_column, "predicate"]
    )[mean_columns].mean().reset_index()
    tmp_df["cpc"] = tmp_df["costs"] / tmp_df["clicks"]

    placement_kpi_df = tmp_df.pivot(
        index=key_columns + [date_column], columns='predicate', values=feat_columns).reset_index()

    columns0 = list(placement_kpi_df.columns.droplevel(0))
    new_columns = list(placement_kpi_df.columns.droplevel(1))
    for i in range(len(new_columns)):
        if columns0[i] != "":
            new_columns[i] = f"{columns0[i]}_{new_columns[i]}"
    placement_kpi_df.columns = new_columns

    for column in require_columns:
        if column not in placement_kpi_df.columns:
            placement_kpi_df[column] = None

    placement_kpi_df = placement_kpi_df.drop(
        set(placement_kpi_df.columns)
        ^ set(
            key_columns +
            [date_column] +
            require_columns
        ),
        axis=1)

    return pd.merge(
        df,
        placement_kpi_df,
        how="left", on=key_columns + [date_column])


def target_kpi(df):
    purpose = df['optimization_purpose']
    purpose_value = df['optimization_purpose_value']
    is_null = pd.isnull(purpose_value) | pd.isnull(purpose)
    output = np.where(
        is_null,
        'NULL',
        purpose.map(
            OPTIMIZATION_PURPOSE_TO_KPI
        ).fillna('NULL')
    )
    return pd.Series(output, index=df.index)


def target_kpi_value(df: pd.DataFrame) -> pd.Series:
    purpose = df['optimization_purpose']
    purpose_value = df['optimization_purpose_value']
    is_null = pd.isnull(purpose_value) | pd.isnull(purpose)
    output = np.where(
        is_null,
        np.nan,
        purpose_value.values
    ).astype(np.float64)
    return pd.Series(output, index=df.index)


def adjust_roas_target(tkpi: pd.Series, tkpi_value: pd.Series) -> pd.Series:
    '''
    Percent to ratio only for roas
    '''
    output = np.where(tkpi == 'ROAS', tkpi_value / 100.0, tkpi_value)
    return output


def purpose(tkpi: pd.DataFrame) -> pd.Series:
    kpi_to_purpose = {'NULL': NULL_PURPOSE_DEFAULT}
    for k, v in OPTIMIZATION_PURPOSE_TO_PURPOSE.items():
        kpi_to_purpose[OPTIMIZATION_PURPOSE_TO_KPI[k]] = v
    return tkpi.map(kpi_to_purpose)


def mode(df: pd.DataFrame) -> pd.Series:
    output = np.where(
        df['optimization_priority_mode_type'].apply(lambda x: 'budget' in x),
        MODE_BUDGET,
        MODE_KPI
    )
    return pd.Series(output, index=df.index)


def ewm(grp_df, halflife, min_periods, agg_query_feature_cols, date_col="date"):
    return grp_df.apply(
        lambda x: x.ewm(
            halflife=halflife,
            times=x[date_col].values,
            min_periods=min_periods
        )[agg_query_feature_cols].mean())


def calc_unit_weekly_cpc_for_cap(df, today):
    start = today - timedelta(days=7)
    end = today

    sum_df = df.loc[
        (df['date'] >= start) & (df['date'] <= end),
        ['advertising_account_id', 'portfolio_id', 'costs', 'clicks']
    ].groupby(
        ['advertising_account_id', 'portfolio_id'], dropna=False).sum().reset_index()

    sum_df["unit_weekly_cpc"] = np.where(
        sum_df["clicks"] > 0,
        sum_df["costs"] / sum_df["clicks"],
        0.0
    )

    return sum_df["unit_weekly_cpc"].values[-1]


def get_C(row):
    if row['target_kpi'] == "ROAS":
        if not pd.isnull(row['target_kpi_value']):
            return 1 / row['target_kpi_value']
        else:
            return None
    else:
        return row['target_kpi_value']
