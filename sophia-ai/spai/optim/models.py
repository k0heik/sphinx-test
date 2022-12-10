from dataclasses import dataclass
import datetime
from typing import Optional, List
from enum import Enum, auto
import numpy as np
import pandas as pd
from .utils import cpc, cpa, inv_roas, ema, nan2none


class KPI(Enum):
    NULL = auto()
    CPC = auto()
    CPA = auto()
    ROAS = auto()

    def __repr__(self):
        return str(self.name)

    def __str__(self):
        return str(self.name)


class Purpose(Enum):
    CLICK = auto()
    CONVERSION = auto()
    SALES = auto()

    def __repr__(self):
        return str(self.name)

    def __str__(self):
        return str(self.name)


class Mode(Enum):
    BUDGET = auto()
    KPI = auto()

    def __repr__(self):
        return str(self.name)

    def __str__(self):
        return str(self.name)


@dataclass
class Performance:
    impressions: int
    clicks: int
    conversions: int
    sales: int
    costs: float
    bidding_price: float
    cpc: float
    cvr: float
    rpc: float
    date: datetime.date

    def as_dict(self):
        return vars(self)

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, Performance):
            return False
        return (
            self.date == o.date and
            self.impressions == o.impressions and
            self.clicks == o.clicks and
            self.conversions == o.conversions and
            self.sales == o.sales and
            self.costs == o.costs and
            self.bidding_price == o.bidding_price and
            self.cpc == o.cpc and
            self.cvr == o.cvr and
            self.rpc == o.rpc
        )

    def __hash__(self):
        return hash(tuple(self.as_dict().values()))


@dataclass
class DataForCPC:
    clicks: int
    costs: float
    date: datetime.date


@dataclass
class PIDConfig:
    '''
    A set of parameters for PID controller
    '''
    sign: float = -1.0
    th_ratio_reduce_oscillate: float = 0.05
    reduce_rate: float = 0.8
    th_ratio_accelerate: float = 5.0
    accelerate_rate: float = 1.2
    ub_ratio_output: float = 1.5
    lb_ratio_output: float = 1/1.5
    not_ml_applied_days_threshold: int = 3  # p,qをリセットするMLが適用されなかった日数の閾値


@dataclass
class OptimiseTargetConfig:
    THRESHOLD_OF_CLICKS_WEEKLY: int = 0
    THRESHOLD_OF_CV_MONTHLY: int = 0
    THRESHOLD_OF_SALES_MONTHLY: int = 0


@dataclass
class BiddingMLConfig:
    BIDDING_UB_RATIO_OVER: float = 1.2
    BIDDING_LB_RATIO_OVER: float = 0.8
    BIDDING_UB_RATIO_SHORT: float = 1.2
    BIDDING_LB_RATIO_SHORT: float = 0.8


@dataclass
class BiddingRuleConfig:
    THRESHOLD_OF_IMPRESSIONS_WEEKLY: int = 50
    CPC_RATIO: float = 2.0
    BIDDING_PRICE_DOWN_RATIO: float = 0.9
    BIDDING_PRICE_UP_RATIO: float = 1.1


@dataclass
class State:
    output: Optional[float] = None
    sum_error: float = 0.0
    error: Optional[float] = None
    kp: float = 0.1
    ki: float = 0.01
    kd: float = 1e-6
    original_output: Optional[float] = None

    def as_dict(self):
        return vars(self)


class Ad:
    def __init__(self, df: pd.DataFrame, ad_type: str, ad_id: int, today_df: pd.DataFrame):
        self.ad_type = ad_type
        self.ad_id = ad_id
        self.performances = []
        self.weight = 0.0
        self.rounded_bidding_price = None
        self.is_enabled_bidding_auto_adjustment = bool(
            df['is_enabled_bidding_auto_adjustment'].values[-1])
        self.cpc_prediction = today_df['cpc'].values[0]
        self.cvr_prediction = today_df['cvr'].values[0]
        self.rpc_prediction = today_df['rpc'].values[0]
        for (impressions, clicks,
             conversions, sales,
             costs, bidding_price, pcpc, cvr, rpc, date) in zip(
                df['impressions'], df['clicks'],
                df['conversions'], df['sales'],
                df['costs'], df['bidding_price'],
                df['cpc'], df['cvr'], df['rpc'], df['date']):
            self.performances.append(
                Performance(
                    impressions=impressions,
                    clicks=clicks,
                    conversions=conversions,
                    sales=sales,
                    costs=costs,
                    bidding_price=bidding_price,
                    cpc=nan2none(pcpc),
                    cvr=nan2none(cvr),
                    rpc=nan2none(rpc),
                    date=date,
                )
            )

    @property
    def last_bidding_price(self):
        return self.performances[-1].bidding_price

    def value(self, purpose: Purpose):
        if purpose is Purpose.CLICK:
            return 1 / self.cpc_prediction
        elif purpose is Purpose.CONVERSION:
            return self.cvr_prediction / self.cpc_prediction
        elif purpose is Purpose.SALES:
            return self.rpc_prediction / self.cpc_prediction

    def delta(self, kpi: KPI):
        if kpi is KPI.NULL:
            return None
        elif kpi is KPI.CPC:
            return 1.0
        elif kpi is KPI.CPA:
            return self.performances[-1].cvr
        elif kpi is KPI.ROAS:
            return self.performances[-1].rpc

    def weekly_clicks(self):
        return sum([p.clicks for p in self.performances[-7:]])

    def monthly_conversion(self):
        return sum([p.conversions for p in self.performances[-28:]])

    def monthly_sales(self):
        return sum([p.sales for p in self.performances[-28:]])

    def ema_cost(self, span=7):
        costs = [p.costs for p in self.performances]
        ema_costs = ema(costs, span)
        return ema_costs[-1]

    def has_predicted_kpi(self):
        if len(self.performances) == 0:
            return False
        pcpc = self.performances[-1].cpc
        cvr = self.performances[-1].cvr
        rpc = self.performances[-1].rpc
        if pd.isnull(pcpc) or pd.isnull(cvr) or pd.isnull(rpc):
            return False
        else:
            return True

    def is_valid(self, purpose: Purpose):
        if purpose is Purpose.CLICK:
            return self.weekly_clicks() > OptimiseTargetConfig.THRESHOLD_OF_CLICKS_WEEKLY
        elif purpose is Purpose.CONVERSION:
            return (
                self.weekly_clicks() > OptimiseTargetConfig.THRESHOLD_OF_CLICKS_WEEKLY and
                self.monthly_conversion() > OptimiseTargetConfig.THRESHOLD_OF_CV_MONTHLY
            )
        elif purpose is Purpose.SALES:
            return (
                self.weekly_clicks() > OptimiseTargetConfig.THRESHOLD_OF_CLICKS_WEEKLY and
                self.monthly_sales() > OptimiseTargetConfig.THRESHOLD_OF_SALES_MONTHLY
            )
        raise ValueError('Invalid Purpose')


class Settings:
    def __init__(self,
                 kpi: KPI,
                 purpose: Purpose,
                 mode: Mode,
                 base_target_cost: float,
                 target_cost: float,
                 yesterday_kpi: float,
                 not_ml_applied_days: Optional[int] = 0,
                 target_kpi_value: Optional[float] = None,
                 ad_unit_weekly_cpc: Optional[float] = None):

        if not isinstance(kpi, KPI):
            raise ValueError(f'Type of kpi must be KPI(Enum), but {type(kpi)}')

        if not isinstance(yesterday_kpi, KPI):
            raise ValueError(f'Type of yesterday_kpi must be KPI(Enum), but {type(yesterday_kpi)}')

        if not isinstance(purpose, Purpose):
            raise ValueError(
                f'Type of purpose must be Purpose(Enum), but {type(purpose)}')

        if not isinstance(mode, Mode):
            raise ValueError(
                f'Type of mode must be Mode(Enum), but {type(mode)}')

        if target_cost < 0:
            raise ValueError('target_cost must be non-negative')

        if base_target_cost < 0:
            raise ValueError('base_target_cost must be non-negative')

        if target_kpi_value is not None and target_kpi_value <= 0:
            raise ValueError('target_kpi_value must be positive')

        self.kpi = kpi
        self.purpose = purpose
        self.mode = mode
        self.base_target_cost = base_target_cost
        self.target_cost = target_cost
        self.not_ml_applied_days = not_ml_applied_days
        self._target_kpi_value = target_kpi_value
        self.ad_unit_weekly_cpc = ad_unit_weekly_cpc
        self.yesterday_kpi = yesterday_kpi

    def obs_kpi(self, p: Performance) -> Optional[float]:
        if self.kpi is KPI.NULL:
            return None
        elif self.kpi is KPI.CPC:
            return cpc(p)
        elif self.kpi is KPI.CPA:
            return cpa(p)
        elif self.kpi is KPI.ROAS:
            return inv_roas(p)

    def _mean_performance(self, ps: List[Performance]):
        return Performance(
            impressions=np.nanmean([p.impressions for p in ps]) if len(ps) > 0 else np.nan,
            clicks=np.nanmean([p.clicks for p in ps]) if len(ps) > 0 else np.nan,
            conversions=np.nanmean([p.conversions for p in ps]) if len(ps) > 0 else np.nan,
            sales=np.nanmean([p.sales for p in ps]) if len(ps) > 0 else np.nan,
            costs=np.nanmean([p.costs for p in ps]) if len(ps) > 0 else np.nan,
            bidding_price=None, cpc=None, cvr=None, rpc=None, date=None,
        )

    def calc_v(self, p: Performance) -> float:
        if self.purpose is Purpose.CLICK:
            return 1.0
        elif self.purpose is Purpose.CONVERSION:
            return p.cvr
        elif self.purpose is Purpose.SALES:
            return p.rpc

    def calc_sigma(self, p: Performance) -> Optional[float]:
        if self.kpi is KPI.NULL:
            return None
        elif self.kpi is KPI.CPC:
            return 1.0
        elif self.kpi is KPI.CPA:
            return p.cvr
        elif self.kpi is KPI.ROAS:
            return p.rpc

    def calc_weight(self, p: Performance) -> float:
        if self.kpi is KPI.NULL:
            raise NotImplementedError
        elif self.kpi is KPI.CPC:
            return p.clicks
        elif self.kpi is KPI.CPA:
            return p.conversions
        elif self.kpi is KPI.ROAS:
            return p.sales

    def is_optimise_target(self, performances: List[Performance]) -> bool:
        """最適化目的とKPIの実績値からPID制御による入札額調整対象かどうかの判定を行う
        return
            True: PID制御による入札額調整を適用する（足切りを行わない）
            False: PID制御による入札額調整を適用しない（足切りを行う）
        """
        click_weekly = sum([p.clicks for p in performances[-7:]])
        monthly_conversion = sum([p.conversions for p in performances[-28:]])
        monthly_sales = sum([p.sales for p in performances[-28:]])

        if self.purpose is Purpose.CLICK:
            # 過去一週間のクリックが0より大きい場合は，
            # PID制御による入札額調整を適用
            return (
                click_weekly > OptimiseTargetConfig.THRESHOLD_OF_CLICKS_WEEKLY
            )
        elif self.purpose is Purpose.CONVERSION:
            # 過去一週間のクリック数が0より大きいかつ，
            # 過去28日間のコンバージョン数が0より大きい場合は
            # PID制御による入札額調整を適用
            return (
                click_weekly >
                OptimiseTargetConfig.THRESHOLD_OF_CLICKS_WEEKLY and
                monthly_conversion >
                OptimiseTargetConfig.THRESHOLD_OF_CV_MONTHLY
            )
        elif self.purpose is Purpose.SALES:
            # 過去一週間のクリック数が0より大きいかつ
            # 過去28日間の売り上げが0より大きい場合は
            # PID制御による入札額調整を適用
            return (
                click_weekly >
                OptimiseTargetConfig.THRESHOLD_OF_CLICKS_WEEKLY and
                monthly_sales >
                OptimiseTargetConfig.THRESHOLD_OF_SALES_MONTHLY
            )
        else:
            raise NotImplementedError

    def as_dict(self):
        return vars(self)

    @property
    def C(self) -> Optional[float]:
        if self.kpi is KPI.ROAS:
            if self._target_kpi_value is not None:
                return 1 / self._target_kpi_value
            else:
                return None
        else:
            return self._target_kpi_value
