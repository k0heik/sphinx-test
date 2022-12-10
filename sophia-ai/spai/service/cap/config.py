NULL_PURPOSE_DEFAULT = 'SALES'

OPTIMIZATION_PURPOSE_TO_PURPOSE = {
    0: 'SALES', 1: 'CONVERSION', 2: 'CLICK'
}

PREPROCESS_COLUMNS = [
    'advertising_account_id',
    'portfolio_id',
    'campaign_id',
    'date',
    'yesterday_costs',
    'purpose',
    'mode',
    'optimization_costs',
    'remaining_days',
    'today_target_cost',
    'today_noboost_target_cost',
    'yesterday_target_cost',
    'weight',
    'ideal_target_cost',
    'yesterday_daily_budget',
    "minimum_daily_budget",
    "maximum_daily_budget",
    "today_coefficient",
    "yesterday_coefficient",
    'C',
    "unit_weekly_ema_costs",
    "unit_ex_observed_C",
    "campaign_weekly_ema_costs",
    "campaign_observed_C_yesterday_in_month",
]

PREPROCESS_DAILY_COLUMNS = [
    'advertising_account_id',
    'portfolio_id',
    'campaign_id',
    'date',
    'clicks',
    'conversions',
    'sales',
    'costs',
]

UNIT_PK_COLS = [
    'advertising_account_id',
    'portfolio_id',
    'date',
]
OUTPUT_COLUMNS = [
    'advertising_account_id',
    'portfolio_id',
    'campaign_id',
    'date',
    'daily_budget_upper',
    'weight',
    'today_target_cost',
    'ideal_target_cost',
    'total_expected_cost',
    'value_of_campaign',
    'gradient',
    'q',
    'max_q',
    'has_potential',
    'yesterday_daily_budget',
    'last_week_max_costs',
    'is_daily_budget_undecidable_unit',
    'unit_weekly_cpc_for_cap',
]
COSTS_WINDOW_SIZE = 7
CLICK_WINDOW_SIZE = 7
CV_WINDOW_SIZE = 28
SALES_WINDOW_SIZE = 28
