TARGET_COLUMN = "cpc"
WEIGHT_COLUMN = "clicks"

LAG_FEATURE_COLS = ['cpc', 'bidding_price']
LAG_AGG_KEY_COLS_MAP = {
    "ad_id": ["ad_type", "ad_id"],
    "campaign_id": ["campaign_id"],
    "unit_id": ["unit_id"],
}
LAG_TERM = 14

FEATURE_COLUMNS = [
    'ad_id_lag10_cpc',
    'ad_id_lag11_cpc',
    'ad_id_lag12_cpc',
    'ad_id_lag13_cpc',
    'ad_id_lag14_cpc',
    'ad_id_lag1_bidding_price',
    'ad_id_lag1_cpc',
    'ad_id_lag2_cpc',
    'ad_id_lag3_cpc',
    'ad_id_lag4_cpc',
    'ad_id_lag5_cpc',
    'ad_id_lag6_cpc',
    'ad_id_lag7_cpc',
    'ad_id_lag8_cpc',
    'ad_id_lag9_cpc',
    'ad_type_feature',
    'campaign_id_lag10_cpc',
    'campaign_id_lag14_cpc',
    'campaign_id_lag1_bidding_price',
    'campaign_id_lag1_cpc',
    'campaign_id_lag6_cpc',
    'campaign_type',
    'match_type',
    'placementProductPage_cpc',
    'placementTop_cpc',
    'targeting_type',
    'unit_id_lag14_bidding_price',
    'unit_id_lag1_cpc',
    'weekday',
]

CATEGORICAL_COLUMNS = [
    'ad_type_feature',
    'match_type',
    'campaign_type',
    'targeting_type',
]
OUTPUT_COLUMNS = [
    "advertising_account_id",
    "portfolio_id",
    "ad_group_id",
    "campaign_id",
    "ad_type",
    "ad_id",
    "date",
    "cpc"
]
THRESHOLD_OF_CLICKS_WEEKLY = 0
THRESHOLD_OF_CV_MONTHLY = 0
