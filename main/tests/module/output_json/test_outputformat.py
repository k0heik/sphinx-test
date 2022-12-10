import random
import pandas as pd
import numpy as np
import pytest

from module.output_json.format import OutputFormat


def _data_test_ml_enable():
    data_cnt = 3
    default_ad_types = ["type", "type", "type2"]
    return {
        "unchange bid, unchange budget (single campaign)": (
            pd.DataFrame(
                {
                    "campaign_id": [1] * data_cnt,
                    "ad_type": default_ad_types,
                    "ad_id": range(1, 1 + data_cnt),
                    "daily_budget": range(1, 1 + data_cnt),
                    "last_daily_budget": range(1, 1 + data_cnt),
                    "bidding_price": range(100, 100 + data_cnt),
                    "is_paused": [False] * data_cnt,
                    "last_bidding_price": range(100, 100 + data_cnt),
                    "is_enabled_bidding_auto_adjustment": [True] * data_cnt,
                    "is_enabled_daily_budget_auto_adjustment": [True] * data_cnt,
                }
            ),
            {
                "result_type": 2,
                "is_ml_enabled": True,
            },
        ),
        "unchange bid, change budget (single campaign)": (
            pd.DataFrame(
                {
                    "campaign_id": [1] * data_cnt,
                    "ad_type": default_ad_types,
                    "ad_id": range(1, 1 + data_cnt),
                    "daily_budget": range(1, 1 + data_cnt),
                    "last_daily_budget": [100, 2, 3],
                    "bidding_price": range(100, 100 + data_cnt),
                    "is_paused": [False] * data_cnt,
                    "last_bidding_price": range(100, 100 + data_cnt),
                    "is_enabled_bidding_auto_adjustment": [True] * data_cnt,
                    "is_enabled_daily_budget_auto_adjustment": [True] * data_cnt,
                }
            ),
            {
                "result_type": 1,
                "is_ml_enabled": True,
                "unit": {
                    "campaigns": [{"campaign_id": "1", "daily_budget": 1, "ads": None}]
                },
            },
        ),
        "change bid, unchange budget (single campaign)": (
            pd.DataFrame(
                {
                    "campaign_id": [1] * data_cnt,
                    "ad_type": default_ad_types,
                    "ad_id": range(1, 1 + data_cnt),
                    "daily_budget": range(1, 1 + data_cnt),
                    "last_daily_budget": range(1, 1 + data_cnt),
                    "bidding_price": range(100, 100 + data_cnt),
                    "is_paused": [False] * data_cnt,
                    "last_bidding_price": range(1000, 1000 + data_cnt),
                    "is_enabled_bidding_auto_adjustment": [True] * data_cnt,
                    "is_enabled_daily_budget_auto_adjustment": [True] * data_cnt,
                }
            ),
            {
                "result_type": 1,
                "is_ml_enabled": True,
                "unit": {
                    "campaigns": [
                        {
                            "campaign_id": "1",
                            "daily_budget": None,
                            "ads": [
                                {
                                    "ad_id": "type_1",
                                    "is_paused": False,
                                    "bidding_price": 100.0,
                                },
                                {
                                    "ad_id": "type_2",
                                    "is_paused": False,
                                    "bidding_price": 101.0,
                                },
                                {
                                    "ad_id": "type2_3",
                                    "is_paused": False,
                                    "bidding_price": 102.0,
                                },
                            ],
                        }
                    ]
                },
            },
        ),
        "change bid, unchange budget (multiple campaigns)": (
            pd.DataFrame(
                {
                    "campaign_id": range(1, 3 + 1),
                    "ad_type": default_ad_types,
                    "ad_id": range(1, 1 + data_cnt),
                    "daily_budget": range(1, 1 + data_cnt),
                    "last_daily_budget": range(1, 1 + data_cnt),
                    "bidding_price": range(100, 100 + data_cnt),
                    "is_paused": [False] * data_cnt,
                    "last_bidding_price": range(1000, 1000 + data_cnt),
                    "is_enabled_bidding_auto_adjustment": [True] * data_cnt,
                    "is_enabled_daily_budget_auto_adjustment": [True] * data_cnt,
                }
            ),
            {
                "result_type": 1,
                "is_ml_enabled": True,
                "unit": {
                    "campaigns": [
                        {
                            "campaign_id": "1",
                            "daily_budget": None,
                            "ads": [
                                {
                                    "ad_id": "type_1",
                                    "is_paused": False,
                                    "bidding_price": 100.0,
                                },
                            ],
                        },
                        {
                            "campaign_id": "2",
                            "daily_budget": None,
                            "ads": [
                                {
                                    "ad_id": "type_2",
                                    "is_paused": False,
                                    "bidding_price": 101.0,
                                },
                            ],
                        },
                        {
                            "campaign_id": "3",
                            "daily_budget": None,
                            "ads": [
                                {
                                    "ad_id": "type2_3",
                                    "is_paused": False,
                                    "bidding_price": 102.0,
                                },
                            ],
                        },
                    ]
                },
            },
        ),
        "change bid, change budget (multiple campaigns)": (
            pd.DataFrame(
                {
                    "campaign_id": range(1, 3 + 1),
                    "ad_type": default_ad_types,
                    "ad_id": range(1, 1 + data_cnt),
                    "daily_budget": range(1, 1 + data_cnt),
                    "last_daily_budget": [9999, 2, 9999],
                    "bidding_price": range(100, 100 + data_cnt),
                    "is_paused": [False] * data_cnt,
                    "last_bidding_price": range(1000, 1000 + data_cnt),
                    "is_enabled_bidding_auto_adjustment": [True] * data_cnt,
                    "is_enabled_daily_budget_auto_adjustment": [True] * data_cnt,
                }
            ),
            {
                "result_type": 1,
                "is_ml_enabled": True,
                "unit": {
                    "campaigns": [
                        {
                            "campaign_id": "1",
                            "daily_budget": 1,
                            "ads": [
                                {
                                    "ad_id": "type_1",
                                    "is_paused": False,
                                    "bidding_price": 100.0,
                                },
                            ],
                        },
                        {
                            "campaign_id": "2",
                            "daily_budget": None,
                            "ads": [
                                {
                                    "ad_id": "type_2",
                                    "is_paused": False,
                                    "bidding_price": 101.0,
                                },
                            ],
                        },
                        {
                            "campaign_id": "3",
                            "daily_budget": 3,
                            "ads": [
                                {
                                    "ad_id": "type2_3",
                                    "is_paused": False,
                                    "bidding_price": 102.0,
                                },
                            ],
                        },
                    ]
                },
            },
        ),
        "missing one of bid (multiple campaigns)": (
            pd.DataFrame(
                {
                    "campaign_id": range(1, 3 + 1),
                    "ad_type": default_ad_types,
                    "ad_id": range(1, 1 + data_cnt),
                    "daily_budget": [1] * data_cnt,
                    "last_daily_budget": [2] * data_cnt,
                    "bidding_price": [1000, np.nan, 1000],
                    "is_paused": [False] * data_cnt,
                    "last_bidding_price": [1000] * data_cnt,
                    "is_enabled_bidding_auto_adjustment": [True] * data_cnt,
                    "is_enabled_daily_budget_auto_adjustment": [True] * data_cnt,
                }
            ),
            None,
        ),
        "missing one of budget (multiple campaigns)": (
            pd.DataFrame(
                {
                    "campaign_id": range(1, 3 + 1),
                    "ad_type": default_ad_types,
                    "ad_id": range(1, 1 + data_cnt),
                    "daily_budget": [1, np.nan, 1],
                    "last_daily_budget": [2] * data_cnt,
                    "bidding_price": [1000] * data_cnt,
                    "is_paused": [False] * data_cnt,
                    "last_bidding_price": [2000] * data_cnt,
                    "is_enabled_bidding_auto_adjustment": [True] * data_cnt,
                    "is_enabled_daily_budget_auto_adjustment": [True] * data_cnt,
                }
            ),
            None,
        ),
        "disabled opt bid of a campaings, change budget (multiple campaigns)": (
            pd.DataFrame(
                {
                    "campaign_id": range(1, 3 + 1),
                    "ad_type": [None] + default_ad_types[1:],
                    "ad_id": [None] + list(range(2, 1 + data_cnt)),
                    "daily_budget": range(1, 1 + data_cnt),
                    "last_daily_budget": [9999, 2, 9999],
                    "bidding_price": [None] + list(range(101, 100 + data_cnt)),
                    "is_paused": [False] * data_cnt,
                    "last_bidding_price": [None] + list(range(1001, 1000 + data_cnt)),
                    "is_enabled_bidding_auto_adjustment": [False]
                    + [True] * (data_cnt - 1),
                    "is_enabled_daily_budget_auto_adjustment": [True] * data_cnt,
                }
            ),
            {
                "result_type": 1,
                "is_ml_enabled": True,
                "unit": {
                    "campaigns": [
                        {
                            "campaign_id": "1",
                            "daily_budget": 1,
                            "ads": None,
                        },
                        {
                            "campaign_id": "2",
                            "daily_budget": None,
                            "ads": [
                                {
                                    "ad_id": "type_2",
                                    "is_paused": False,
                                    "bidding_price": 101.0,
                                },
                            ],
                        },
                        {
                            "campaign_id": "3",
                            "daily_budget": 3,
                            "ads": [
                                {
                                    "ad_id": "type2_3",
                                    "is_paused": False,
                                    "bidding_price": 102.0,
                                },
                            ],
                        },
                    ]
                },
            },
        ),
        "target pause is True": (
            pd.DataFrame(
                {
                    "campaign_id": range(1, 3 + 1),
                    "ad_type": default_ad_types,
                    "ad_id": range(1, 1 + data_cnt),
                    "daily_budget": [1] * data_cnt,
                    "last_daily_budget": [1] * data_cnt,
                    "bidding_price": [2] * data_cnt,
                    "last_bidding_price": [2] * data_cnt,
                    "is_paused": [True] * data_cnt,
                    "is_enabled_bidding_auto_adjustment": [True] * data_cnt,
                    "is_enabled_daily_budget_auto_adjustment": [True] * data_cnt,
                }
            ),
            {
                "result_type": 1,
                "is_ml_enabled": True,
                "unit": {
                    "campaigns": [
                        {
                            "campaign_id": "1",
                            "daily_budget": None,
                            "ads": [
                                {
                                    "ad_id": "type_1",
                                    "is_paused": True,
                                    "bidding_price": 2.0,
                                },
                            ],
                        },
                        {
                            "campaign_id": "2",
                            "daily_budget": None,
                            "ads": [
                                {
                                    "ad_id": "type_2",
                                    "is_paused": True,
                                    "bidding_price": 2.0,
                                },
                            ],
                        },
                        {
                            "campaign_id": "3",
                            "daily_budget": None,
                            "ads": [
                                {
                                    "ad_id": "type2_3",
                                    "is_paused": True,
                                    "bidding_price": 2.0,
                                },
                            ],
                        },
                    ]
                },
            },
        ),
    }


def _data_test_format_exception():
    return {
        "missing one of budget (multiple campaigns)": pd.DataFrame(
            {
                "campaign_id": [np.nan],
                "ad_type": [None],
                "ad_id": [None],
                "daily_budget": [np.nan],
                "last_daily_budget": [np.nan],
                "bidding_price": [np.nan],
                "is_paused": [False],
                "last_bidding_price": [np.nan],
                "is_enabled_bidding_auto_adjustment": [np.nan],
                "is_enabled_daily_budget_auto_adjustment": [np.nan],
            }
        ),
    }


@pytest.mark.parametrize(
    "in_df, out_dict",
    _data_test_ml_enable().values(),
    ids=list(_data_test_ml_enable().keys()),
)
@pytest.mark.parametrize("portfolio_id", [random.randint(1, 10**6), None])
def test_ml_enable(portfolio_id, in_df, out_dict):
    advertising_account_id = random.randint(1, 10**6)
    in_df["advertising_account_id"] = advertising_account_id
    in_df["portfolio_id"] = portfolio_id
    d = OutputFormat(advertising_account_id, portfolio_id, in_df).get_formatted()
    assert out_dict == d


@pytest.mark.parametrize(
    "in_df",
    _data_test_format_exception().values(),
    ids=list(_data_test_format_exception().keys()),
)
@pytest.mark.parametrize("portfolio_id", [random.randint(1, 10**6), None])
def test_format_exception(portfolio_id, in_df):
    advertising_account_id = random.randint(1, 10**6)
    in_df["advertising_account_id"] = advertising_account_id
    in_df["portfolio_id"] = portfolio_id
    with pytest.raises(Exception):
        _ = OutputFormat(advertising_account_id, portfolio_id, in_df).get_formatted()
