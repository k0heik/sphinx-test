WEIGHT_COLUMN = "conversions"
TARGET_COLUMN = "spa"

LAG_FEATURE_COLS = ['spa']
LAG_AGG_KEY_COLS_MAP = {
    "ad_id": ["ad_type", "ad_id"],
}
LAG_TERM = 3

FEATURE_COLUMNS = [
    'ad_id_weekly_clicks',
    'ad_id_weekly_costs',
    'ad_id_weekly_conversions',
    'ad_id_weekly_sales',
    'ad_id_monthly_conversions',
    'ad_id_monthly_sales',
    'ad_id_monthly_cvr',
    'ad_id_monthly_rpc',
    'ad_id_monthly_spa',
    'campaign_id_monthly_spa',
    'campaign_id_ewm7_cvr',
    'unit_id_monthly_spa',
    'unit_id_monthly_cvr',
    'diff_campaign_id_ewm7_cvr',
    'diff_campaign_id_weekly_cvr',
    'diff_campaign_id_weekly_rpc',
    'diff_unit_id_weekly_clicks',
    'diff_unit_id_weekly_cvr',
    'diff_unit_id_weekly_rpc',
    'ad_id_ewm7_query_conversions',
    'ad_id_ewm28_query_conversions',
    'ad_id_lag1_spa',
    'ad_id_lag2_spa',
    'ad_id_lag3_spa',
]
CATEGORICAL_COLUMNS = []
OUTPUT_COLUMNS = [
    "advertising_account_id",
    "portfolio_id",
    "ad_group_id",
    "campaign_id",
    "ad_type",
    "ad_id",
    "date",
    "spa",
]
THRESHOLD_OF_CLICKS_WEEKLY = 0
THRESHOLD_OF_SALES_MONTHLY = 0
