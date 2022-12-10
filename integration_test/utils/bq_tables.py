import datetime
from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class CreatedAndUpdatedAt:
    created_at: datetime.datetime = field(default_factory=datetime.datetime.now, init=False)
    updated_at: datetime.datetime = field(default_factory=datetime.datetime.now, init=False)


@dataclass(frozen=True)
class AdvertisingAccount(CreatedAndUpdatedAt):
    id: int
    amazon_id: int
    amazon_entity_id: int
    name: str
    amazon_account_id: int
    company_id: int
    type: str  # {seller, agency, vendor}
    is_modified_name: bool
    auto_stop_threshold_percent: int
    is_enabled_auto_daily_budget_adjustment: bool
    is_enabled_auto_bid_adjustment: bool
    country_code: str
    currency_code: str
    timezone: str
    is_valid_for_advertising_api: bool


@dataclass(frozen=True)
class Portfolio(CreatedAndUpdatedAt):
    id: int
    name: str
    advertising_account_id: int
    auto_stop_threshold_percent: int
    is_enabled_auto_daily_budget_adjustment: bool
    is_enabled_auto_bid_adjustment: bool


@dataclass(frozen=True)
class Campaign(CreatedAndUpdatedAt):
    id: int
    amazon_id: int
    name: str
    advertising_account_id: int
    portfolio_id: int
    status: str  # {paused, enabled, archived}
    type: str  # {sp, hsa, sd}
    targeting_type: str  # {auto, manual}
    start_at: datetime.datetime
    end_at: datetime.datetime
    budget: float
    budget_type: str  # {daily, lifetime}
    bidding_strategy: Optional[
        str
    ]  # {null, legacyForSales, manual, autoForSales}
    tag_id: int
    auto_campaign_id: int
    ad_format: Optional[str]  # {null, productCollection, video}
    serving_status: Optional[str]  # {null, CAMPAIGN_STATUS_ENABLED}


@dataclass(frozen=True)
class AdGroup(CreatedAndUpdatedAt):
    id: int
    amazon_id: int
    campaign_id: int
    name: str
    status: str  # {paused, enabled, archived}
    default_bid: float
    tag_id: int


@dataclass(frozen=True)
class Ad(CreatedAndUpdatedAt):
    id: int
    amazon_id: int
    campaign_id: int
    ad_group_id: int
    status: str  # {paused, enabled, archived}
    bidding_price: float
    deviation_value_of_roas: float
    tag_id: int


@dataclass(frozen=True)
class Keyword(Ad):
    amazon_ad_group_id: int
    keyword_text: str
    match_type: str  # {broad, exact, phrase}
    nomination_type_id: int


@dataclass(frozen=True)
class KeywordQuery(CreatedAndUpdatedAt):
    id: int
    keyword_id: int
    query: str


@dataclass(frozen=True)
class ProductTargeting(Ad):
    expression_type: str  # {manual, auto}


@dataclass(frozen=True)
class Targeting(Ad):
    amazon_ad_group_id: int
    # expression: queryHighRelMatches, asinSubstituteRelated, asinAccessoryRelated
    # queryBroadRelMatches, asinSameAs, asinCategorySameAs
    expression: str


@dataclass(frozen=True)
class Daily(CreatedAndUpdatedAt):
    date: datetime.date
    id: int
    bidding_price: float
    impressions: int
    clicks: int
    costs: float
    conversions: int
    sales: float
    orders_new_to_brand: int
    sales_new_to_brand: float


@dataclass(frozen=True)
class DailyKeyword(Daily):
    keyword_id: int
    campaign_id: int


@dataclass(frozen=True)
class DailyKeywordQuery(Daily):
    keyword_query_id: int


@dataclass(frozen=True)
class DailyProductTargeting(Daily):
    product_targeting_id: int


@dataclass(frozen=True)
class DailyTargeting(Daily):
    targeting_id: int


@dataclass(frozen=True)
class DailyAdGroup(Daily):
    ad_group_id: int


@dataclass(frozen=True)
class DailyCampaign(Daily):
    campaign_id: int


@dataclass(frozen=True)
class OptimizationAdSetting(CreatedAndUpdatedAt):
    id: int
    ad_id: int
    ad_type: str  # {Keyword, Targeting, ProductTargeting, AdGroup}
    is_bid_optimization: bool


@dataclass(frozen=True)
class OptimizationSetting(CreatedAndUpdatedAt):
    id: int
    optimization_costs: float
    is_auto_apply_bid_changes: bool
    is_auto_apply_daily_budget_changes: bool
    is_auto_apply_pause_keywords: bool
    optimization_priority_mode_type: str  # {budget, goal, cost}
    optimization_purpose: Optional[int]  # {0, 1, 2} where 0=roas, 1=cpa, 2=cpc
    optimization_purpose_value: Optional[float]
    optimization_completed_at: datetime.datetime
    portfolio_id: int


# injected by etl
@dataclass(frozen=True)
class BiddingUnitInfo:
    data_date: datetime.date
    advertising_account_id: int
    portfolio_id: int
    purpose: str  # {sales, conversion, click}
    target_cost: float  # required for mode='cost'
    type: str  # {month, range}
    mode: str  # {goal, budget, cost}
    start: datetime.date
    end: datetime.date
    total_budget: int  # required for mode='budget'
    target_value: float  # required for mode='goal'
    remaining_days: int
    change_date: datetime.date
    round_up_point: int


@dataclass(frozen=True)
class BiddingAdPerformance:
    data_date: datetime.date
    advertising_account_id: int
    portfolio_id: int
    campaign_id: int
    ad_type: str  # {targeting, product_targeting, keyword, ad_group}
    ad_id: int
    daily_budget: float
    minimum_daily_budget: float
    maximum_daily_budget: float
    is_enabled_daily_budget_auto_adjustment: bool
    is_enabled_bidding_auto_adjustment: bool
    minimum_bidding_price: float
    maximum_bidding_price: float
    date: datetime.date
    impressions: int
    clicks: int
    conversions: int
    sales: float
    costs: float
    bidding_price: float


@dataclass(frozen=True)
class AdGroupHistory(CreatedAndUpdatedAt):
    id: int
    ad_group_id: int
    reported_at: datetime.datetime
    default_bid: float
    amazon_default_bid: float


@dataclass(frozen=True)
class BiddingHistory(CreatedAndUpdatedAt):
    id: int
    reported_at: datetime.datetime
    bidding_price: float
    amazon_bidding_price: float


@dataclass(frozen=True)
class KeywordHistory(BiddingHistory):
    keyword_id: int


@dataclass(frozen=True)
class ProductTargetingHistory(BiddingHistory):
    product_targeting_id: int


@dataclass(frozen=True)
class TargetingHistory(BiddingHistory):
    targeting_id: int


@dataclass(frozen=True)
class MLResultUnit:
    advertising_account_id: int
    portfolio_id: Optional[int]
    date: datetime.date
    is_ml_enabled: Optional[bool]
    is_lastday_ml_applied: Optional[bool]
    p: Optional[float]
    q: Optional[float]
    p_kp: Optional[float]
    p_ki: Optional[float]
    p_kd: Optional[float]
    p_error: Optional[float]
    p_sum_error: Optional[float]
    q_kp: Optional[float]
    q_ki: Optional[float]
    q_kd: Optional[float]
    q_error: Optional[float]
    q_sum_error: Optional[float]
    target_cost: Optional[float]
    target_kpi: Optional[str]


@dataclass(frozen=True)
class MLResultCampaign:
    advertising_account_id: int
    portfolio_id: Optional[int]
    campaign_id: int
    date: datetime.date
    cap_daily_budget: Optional[float]
    cap_daily_budget_weight: Optional[float]


@dataclass(frozen=True)
class MLResultAd:
    advertising_account_id: int
    portfolio_id: Optional[int]
    campaign_id: int
    ad_type: str
    ad_id: int
    date: datetime.date
    bidding_price: Optional[float]
