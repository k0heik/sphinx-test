from functools import lru_cache
from typing import Tuple

import numpy as np

from spai.utils.kpi.kpi import (
    MODE_BUDGET,
)
from .models import (
    Performance,
    KPI,
    Purpose,
    Mode,
    Settings,
    State,
)
from .utils import (
    nan2none,
    nan2zero,
    nan2const,
    weighted_cpc,
)


KP_DEFAULT = 0.1
KI_DEFAULT = 0.01
KD_DEFAULT = 1e-6

CPC_LIMIT_RATE = 3.0
CPC_PERIOD_DAYS = 14


def _kpi_value(target_kpi):
    if target_kpi == 'CPC':
        return KPI.CPC
    elif target_kpi == 'CPA':
        return KPI.CPA
    elif target_kpi == 'ROAS':
        return KPI.ROAS

    return KPI.NULL


def dict2settings(d: dict) -> Settings:
    mode = Mode.BUDGET if d['mode'] == MODE_BUDGET else Mode.KPI
    kpi = _kpi_value(d['target_kpi'])
    yesterday_kpi = _kpi_value(d['yesterday_target_kpi'])
    purpose = Purpose.SALES  # default
    if d['purpose'] == 'CONVERSION':
        purpose = Purpose.CONVERSION
    elif d['purpose'] == 'CLICK':
        purpose = Purpose.CLICK
    target_kpi_value = d['target_kpi_value']
    if np.isnan(target_kpi_value):
        target_kpi_value = None

    not_ml_applied_days = int(d['not_ml_applied_days'])

    settings = Settings(kpi=kpi,
                        purpose=purpose,
                        mode=mode,
                        base_target_cost=d['base_target_cost'],
                        target_cost=d['target_cost'],
                        target_kpi_value=target_kpi_value,
                        not_ml_applied_days=not_ml_applied_days,
                        yesterday_kpi=yesterday_kpi,
                        )
    return settings


def dict2qpstates(d: dict) -> Tuple[State, State]:
    p = nan2none(d['p'])
    q = nan2none(d['q'])
    p_error = nan2none(d['p_error'])
    q_error = nan2none(d['q_error'])

    p_sum_error = nan2zero(d['p_sum_error'])
    q_sum_error = nan2zero(d['q_sum_error'])

    p_kp = nan2const(d['p_kp'], KP_DEFAULT)
    p_ki = nan2const(d['p_ki'], KI_DEFAULT)
    p_kd = nan2const(d['p_kd'], KD_DEFAULT)
    q_kp = nan2const(d['q_kp'], KP_DEFAULT)
    q_ki = nan2const(d['q_ki'], KI_DEFAULT)
    q_kd = nan2const(d['q_kd'], KD_DEFAULT)

    p_state = State(p, p_sum_error, p_error, p_kp, p_ki, p_kd)
    q_state = State(q, q_sum_error, q_error, q_kp, q_ki, q_kd)
    return p_state, q_state


def dict2perf(d: dict) -> Performance:
    return Performance(impressions=d['impressions'],
                       clicks=d['clicks'],
                       conversions=d['conversions'],
                       sales=d['sales'],
                       costs=d['costs'],
                       bidding_price=d['bidding_price'],
                       cpc=nan2none(d['cpc']),
                       cvr=nan2none(d['cvr']),
                       rpc=nan2none(d['rpc']),
                       date=d['date'],
                       )


@lru_cache(maxsize=None)
def estimate_ad_cpc(ws: int,
                    ad_historical_performances: Tuple[Performance]):
    clicks = np.array([p.clicks for p in ad_historical_performances])
    costs = np.array([p.costs for p in ad_historical_performances])
    return weighted_cpc(ws, clicks, costs)


def calc_bid_cpc_limit(
    ad_historical_performances: Tuple[Performance],
    unit_period_cpc: float,
):
    ad_ema_period_cpc = estimate_ad_cpc(CPC_PERIOD_DAYS, ad_historical_performances)

    # 広告の1週間のCPCが0以上だった場合
    if ad_ema_period_cpc > 0:
        return max(ad_ema_period_cpc * CPC_LIMIT_RATE, unit_period_cpc * CPC_LIMIT_RATE), unit_period_cpc
    # ユニットの1週間のCPCが0だった場合
    elif unit_period_cpc > 0:
        return unit_period_cpc * CPC_LIMIT_RATE, unit_period_cpc
    else:
        return None, None
