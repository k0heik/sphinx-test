import datetime
import random
import pandas as pd
import pytest

from module import config, prepare_df
from spai.service.pid.config import OUTPUT_COLUMNS as PID_OUTPUT_COLUMNS


@pytest.fixture
def common_data_today():
    return datetime.datetime(2021, 4, 28)


@pytest.fixture
def common_data_yesterday():
    return datetime.datetime(2021, 4, 27)


@pytest.fixture
def common_data_actual_days():
    return 28


@pytest.fixture
def common_data_ad_num():
    return 2


@pytest.fixture
def unit_info_df():
    return prepare_df.add_unit_info_setting_columns(
        pd.DataFrame(
            {
                "advertising_account_id": [1],
                "portfolio_id": [None],
                "unit_param": ["unit_param"],
                "account_type": ["account_type"],
                "optimization_costs": [1],
                "optimization_purpose": ["optimization_purpose"],
                "optimization_priority_mode_type": ["optimization_priority_mode_type"],
                "optimization_purpose_value": [1],
                "start": [datetime.datetime(1970, 1, 1)],
                "round_up_point": [1],
                "target_kpi": ["target_kp"],
                "target_kpi_value": 1,
                "mode": ["mode"],
                "purpose": ["purpose"],
            }
        ).astype(config.UNIT_KEY_DTYPES)
    )


@pytest.fixture
def campaign_info_df():
    return pd.DataFrame(
        {
            "advertising_account_id": [1],
            "portfolio_id": [None],
            "campaign_id": [1],
            "campaign_param": ["campaign_param"],
            "budget": [1],
            "budget_type": ["budget_type"],
        }
    ).astype(config.UNIT_KEY_DTYPES)


@pytest.fixture
def ad_info_df(common_data_ad_num):
    return pd.DataFrame(
        {
            "advertising_account_id": [1] * common_data_ad_num,
            "portfolio_id": [None] * common_data_ad_num,
            "ad_group_id": [1] * common_data_ad_num,
            "campaign_id": [1] * common_data_ad_num,
            "ad_type": ["ad_type"] * common_data_ad_num,
            "ad_id": [1, 2],
            "ad_params": ["ad_params_1", "ad_params_2"],
            "impressions": [1] * common_data_ad_num,
            "clicks": [1] * common_data_ad_num,
            "conversions": [1] * common_data_ad_num,
            "sales": [1] * common_data_ad_num,
            "costs": [1] * common_data_ad_num,
            "bidding_price": [100] * common_data_ad_num,
            "match_type": ["match_type"] * common_data_ad_num,
            "is_enabled_bidding_auto_adjustment": [True] * common_data_ad_num,
        }
    ).astype(config.UNIT_KEY_DTYPES)


@pytest.fixture
def campaign_all_actual_df(common_data_yesterday, common_data_actual_days):
    df = pd.DataFrame(
        {
            "campaign_id": [1] * common_data_actual_days,
            "date": pd.date_range(
                end=common_data_yesterday, freq="D", periods=common_data_actual_days
            ),
            "impressions": [1] * common_data_actual_days,
            "clicks": [1] * common_data_actual_days,
            "conversions": [1] * common_data_actual_days,
            "sales": [1] * common_data_actual_days,
            "costs": [1] * common_data_actual_days,
            "actual_param": ["campaign_actual_param"] * common_data_actual_days,
        }
    )
    tmp_df = df.copy()
    tmp_df["campaign_id"] = 99

    return pd.concat([df, tmp_df]).reset_index()


@pytest.fixture
def ad_target_actual_df(
    common_data_yesterday, common_data_actual_days, common_data_ad_num
):
    df = pd.DataFrame(
        {
            "ad_type": ["ad_type"] * common_data_actual_days,
            "ad_id": [1] * common_data_actual_days,
            "date": pd.date_range(
                end=common_data_yesterday, freq="D", periods=common_data_actual_days
            ),
            "actual_param": ["ad_actual_param"] * common_data_actual_days,
            "bidding_price": [999] * common_data_actual_days,
        }
    )
    dfs = [df]
    for i in range(common_data_ad_num - 1):
        tmp_df = df.copy()
        tmp_df["ad_id"] = df["ad_id"].max() + i + 1
        dfs.append(tmp_df)

    return pd.concat(dfs).reset_index()


@pytest.fixture
def ad_input_json_df(common_data_yesterday):
    days = 30
    return pd.DataFrame(
        {
            "advertising_account_id": [1] * days,
            "portfolio_id": [None] * days,
            "campaign_id": [1] * days,
            "ad_type": ["ad_type"] * days,
            "ad_id": [1] * days,
            "date": pd.date_range(end=common_data_yesterday, freq="D", periods=days),
            "bidding_price": [100] * days,
            "daily_budget": [100] * days,
            "minimum_bidding_price": [100] * days,
            "maximum_bidding_price": [100] * days,
            "minimum_daily_budget": [100] * days,
            "maximum_daily_budget": [100] * days,
            "is_enabled_bidding_auto_adjustment": [True] * days,
            "is_enabled_daily_budget_auto_adjustment": [True] * days,
            "impressions": [100] * days,
            "clicks": [100] * days,
            "conversions": [100] * days,
            "sales": [100] * days,
            "costs": [100] * days,
        }
    ).astype(config.UNIT_KEY_DTYPES)


@pytest.fixture
def lastday_ml_result_unit_df(ad_target_actual_df):
    dates = ad_target_actual_df.groupby("date").last().reset_index()["date"].values
    days = 1
    return pd.DataFrame(
        {
            "date": dates[-1],
            "p": [random.random() for _ in range(days)],
            "q": [random.random() for _ in range(days)],
            "p_kp": [random.random() for _ in range(days)],
            "p_ki": [random.random() for _ in range(days)],
            "p_kd": [random.random() for _ in range(days)],
            "p_error": [random.random() for _ in range(days)],
            "p_sum_error": [random.random() for _ in range(days)],
            "q_kp": [random.random() for _ in range(days)],
            "q_ki": [random.random() for _ in range(days)],
            "q_kd": [random.random() for _ in range(days)],
            "q_error": [random.random() for _ in range(days)],
            "q_sum_error": [random.random() for _ in range(days)],
            "target_cost": [1000] * days,
            "target_kpi": ["ROAS"] * days,
        }
    )


@pytest.fixture
def daily_budget_boost_coefficient_df():
    return pd.DataFrame(
        {
            "start_date": pd.date_range("1970-01-01", "1970-01-02"),
            "end_date": pd.date_range("1970-01-01", "1970-01-02"),
            "coefficient": [0.0] * 2,
            "updated_at": [datetime.datetime(2022, 1, 1)] * 2,
        }
    )


@pytest.fixture
def cpc_prediction_df(ad_target_actual_df, common_data_today):
    ad_keys_df = (
        ad_target_actual_df[["ad_type", "ad_id"]]
        .groupby(["ad_type", "ad_id"])
        .last()
        .reset_index()
    )
    n = len(ad_keys_df)

    return pd.DataFrame(
        {
            "ad_type": ad_keys_df["ad_type"],
            "ad_id": ad_keys_df["ad_id"],
            "date": [common_data_today] * n,
            "cpc": [0.1] * n,
        }
    )


@pytest.fixture
def cvr_prediction_df(cpc_prediction_df):
    return cpc_prediction_df.rename(columns={"cpc": "cvr"})


@pytest.fixture
def spa_prediction_df(cpc_prediction_df):
    return cpc_prediction_df.rename(columns={"cpc": "spa"})


@pytest.fixture
def pid_controller_df(common_data_today):
    n = 1
    return pd.DataFrame(
        {
            "advertising_account_id": [1] * n,
            "portfolio_id": [None] * n,
            "date": [common_data_today] * n,
            'target_cost': [100] * n,
            'base_target_cost': [100] * n,
            "p": [0.1] * n,
            "q": [0.1] * n,
            "pre_reupdate_p": [0.11] * n,
            "pre_reupdate_q": [0.11] * n,
            "p_kp": [0.1] * n,
            "p_ki": [0.1] * n,
            "p_kd": [0.1] * n,
            "p_error": [0.1] * n,
            "p_sum_error": [0.1] * n,
            "q_kp": [0.1] * n,
            "q_ki": [0.1] * n,
            "q_kd": [0.1] * n,
            "q_error": [0.1] * n,
            "q_sum_error": [0.1] * n,
            "origin_p": [0.1] * n,
            "origin_q": [0.1] * n,
            'error': [False] * n,
            'is_updated': [True] * n,
            'is_pid_initialized': [True] * n,
            'is_skip_pid_calc_state': [False] * n,
            'obs_kpi': [0.1] * n,
            'valid_ads_num': [1] * n,
        }
    ).astype(config.UNIT_KEY_DTYPES)[PID_OUTPUT_COLUMNS]
