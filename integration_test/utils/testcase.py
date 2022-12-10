from __future__ import annotations
from typing import List, Optional
import datetime

from pydantic import (
    BaseModel as PydanticBaseModel,
    validator
)


class BaseModel(PydanticBaseModel):
    class Config:
        arbitrary_types_allowed = True


class Performance(BaseModel):
    impressions: int
    clicks: int
    conversions: int
    sales: float
    costs: float


class PerformanceHistory(BaseModel):
    day: int
    performance: Performance


class PidResult(BaseModel):
    p: float
    q: float
    p_kp: float
    p_kd: float
    p_ki: float
    q_kp: float
    q_kd: float
    q_ki: float
    p_error: float
    p_sum_error: float
    q_error: float
    q_sum_error: float


class Unit(BaseModel):
    mode: str
    purpose: str
    target_kpi_value: float
    optimization_costs: float
    is_opt_enabled: bool
    yesterday_pid_result: PidResult
    yesterday_target_cost: float


class CampaignDetail(BaseModel):
    id: int
    minimum_daily_budget: float
    maximum_daily_budget: float
    current_daily_budget: float
    yesterday_daily_budget_weight: float
    performance: Performance


class Campaign(BaseModel):
    list: List[CampaignDetail]


class AdDetail(BaseModel):
    id: int
    campaign_id: int
    ad_type: str
    is_enabled_bidding_auto_adjustment: bool
    performance: Performance
    current_bidding_price: float
    today_predicted_cpc: float
    today_predicted_cvr: float
    today_predicted_spa: float
    minimum_bidding_price: float
    maximum_bidding_price: float


class Ad(BaseModel):
    list: List[AdDetail]


class Validations(BaseModel):
    bid_direction: Optional[int]
    budget_direction: Optional[int]


class TestCase(BaseModel):
    id: int
    advertising_account_id: int
    portfolio_id: Optional[int]
    name: str
    description: str
    processing_date: datetime.datetime
    history_days: int
    num_past_days: int
    is_lastday_ml_applied: bool
    is_mock_kpi_prediction: bool
    unit: Unit
    campaign: Campaign
    ad: Ad
    validations: Validations

    @validator("processing_date", pre=True)
    def parse_processing_date(cls, value):
        return datetime.datetime.strptime(
            value,
            "%Y-%m-%d"
        )
