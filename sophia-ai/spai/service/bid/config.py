from dataclasses import dataclass

FILLNA_COLUMNS = {
    'impressions',
    'clicks',
    'costs',
    'conversions',
    'sales',
}

OUTPUT_DTYPES = {
    'advertising_account_id': 'int64',
    'portfolio_id': 'Int64',
    'campaign_id': 'int64',
    'ad_type': 'str',
    'ad_id': 'int64',
    'date': 'datetime64[ns]',
    'is_ml_applied': 'bool',
    'bidding_price': 'float64',
    'origin_bidding_price': 'float64',
    'unit_cpc': 'float64',
    'bidding_algorithm': 'str',
    'has_exception': 'bool',
    'is_provisional_bidding': 'bool',
    'ad_value': 'float',
    'ad_ema_weekly_cpc': 'float',
    'sum_click_last_four_weeks': 'float',
    'sum_cost_last_four_weeks': 'float',
    'cpc_last_four_weeks': 'float',
}

CPC_LIMIT_RATE = 3.0


@dataclass
class OptimiseTargetConfig:
    THRESHOLD_OF_CLICKS_WEEKLY: int = 0
    THRESHOLD_OF_CV_MONTHLY: int = 0
    THRESHOLD_OF_SALES_MONTHLY: int = 0


@dataclass
class BiddingRuleConfig:
    THRESHOLD_OF_IMPRESSIONS_WEEKLY: int = 50
    CPC_RATIO: float = 2.0
    BIDDING_PRICE_DOWN_RATIO: float = 0.9
    BIDDING_PRICE_UP_RATIO: float = 1.1


@dataclass
class BiddingMLConfig:
    BIDDING_UB_RATIO_OVER: float = 1.2
    BIDDING_LB_RATIO_OVER: float = 0.8
    BIDDING_UB_RATIO_SHORT: float = 1.2
    BIDDING_LB_RATIO_SHORT: float = 0.8
