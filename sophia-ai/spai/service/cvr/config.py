TARGET_COLUMN = "cvr"
WEIGHT_COLUMN = "clicks"

LAG_FEATURE_COLS = ['cvr']
LAG_AGG_KEY_COLS_MAP = {
    "ad_id": ["ad_type", "ad_id"],
}
LAG_TERM = 1

FEATURE_COLUMNS = [
    'campaign_type',
    'ad_id_ewm7_cvr',
    'ad_id_weekly_clicks',
    'ad_id_weekly_costs',
    'ad_id_weekly_conversions',
    'ad_id_weekly_sales',
    'ad_id_weekly_ctr',
    'ad_id_weekly_cvr',
    'ad_id_monthly_conversions',
    'ad_id_monthly_sales',
    'ad_id_monthly_ctr',
    'ad_id_monthly_rpc',
    'campaign_id_weekly_cvr',
    'campaign_id_monthly_cvr',
    'campaign_id_monthly_rpc',
    'unit_id_ewm7_rpc',
    'unit_id_weekly_cvr',
    'diff_campaign_id_monthly_ctr',
    'diff_unit_id_ewm7_ctr',
    'diff_unit_id_weekly_ctr',
    'ad_id_ewm7_query_clicks',
    'ad_id_ewm28_query_clicks',
    'ad_id_lag1_cvr',
]
CATEGORICAL_COLUMNS = [
    "campaign_type",
]
OUTPUT_COLUMNS = [
    "advertising_account_id",
    "portfolio_id",
    "ad_group_id",
    "campaign_id",
    "ad_type",
    "ad_id",
    "date",
    "cvr"
]
THRESHOLD_OF_CLICKS_WEEKLY = 0
THRESHOLD_OF_CV_MONTHLY = 0
