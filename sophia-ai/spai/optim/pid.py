import datetime
import copy
from dataclasses import replace
from typing import Optional, List, Tuple
import numpy as np
import pandas as pd

from .utils import clip, safe_exp, weighted_cpc_by_df
from .pid_util import (
    dict2settings,
    dict2qpstates,
    dict2perf,
    calc_bid_cpc_limit,
    CPC_PERIOD_DAYS,
)
from .models import Settings, State, PIDConfig, Mode, KPI, Ad
from logging import getLogger


logger = getLogger(__name__)
WINDOW_SIZE_FOR_COST = 7
WINDOW_SIZE_FOR_CLICK = 7

LATEST_KPI_BOUND_BUFF = 1.5


def isnan(x):
    if x is None:
        return True
    try:
        return np.isnan(x)
    except TypeError:
        return False


def _is_init_pq(
        settings: Settings, p_state: State, q_state: State,
        not_ml_applied_days_threshold: int = 3) -> bool:
    # ３日間連続でMLが適用されなかった場合
    if settings.not_ml_applied_days >= not_ml_applied_days_threshold:
        return True

    # pの初期値が存在しない場合
    if p_state.output is None:
        return True

    # KPI制約ありでqの初期値が存在しない場合
    if (q_state is not None and
            settings.kpi is not KPI.NULL and
            q_state.output is None):
        return True

    # 前日と制約の指定が異なっている場合
    if (settings.kpi != settings.yesterday_kpi):
        return True

    return False


def _pid_step(
        config: PIDConfig, state: State,
        target: float, observed: float,
        is_begin_of_month: bool = True,
        weight: float = 1.0, sum_weight: float = 1.0,
        ) -> State:
    assert not pd.isnull(state.output), 'State.output must be float'
    if weight < 0:
        raise ValueError('weight must be greater than or equal to 0')
    if sum_weight < 0:
        raise ValueError('sum_weight must be greater than or equal to 0')
    if sum_weight < weight:
        raise ValueError('weight must be less than or equal to sum_weight')
    if sum_weight == 0.0:
        sum_weight = 1.0
        weight = 1.0
    new_state = replace(state)

    if is_begin_of_month:
        sum_weight = weight = 1.0

    new_state.error = error = target - observed
    new_state.sum_error = sum_error = \
        state.sum_error + error if not is_begin_of_month else error
    last_error = state.error
    delta_error = (error - last_error) \
        if last_error and not is_begin_of_month else 0.0

    def update(s: State):
        p_term = s.kp * error
        i_term = s.ki * sum_error
        d_term = s.kd * delta_error
        d_output = config.sign * (p_term + i_term + d_term)
        return d_output

    lr = 1.0
    if last_error and error * last_error < 0:
        new_state.sum_error = sum_error = 0.0
        # Suppress oscillating when oscillating
        if abs(error) > target * config.th_ratio_reduce_oscillate:
            lr *= config.reduce_rate

    # Accelerate to converge faster
    if abs(sum_error) > target * config.th_ratio_accelerate:
        lr *= config.accelerate_rate

    new_state.kp = state.kp * lr
    new_state.ki = state.ki * lr
    new_state.kd = state.kd * lr

    d_output = update(new_state)

    # Adjust kp, ki and kd to reduce step size
    alpha = 1.0
    if safe_exp(d_output) > config.ub_ratio_output:
        alpha = np.log(config.ub_ratio_output) / d_output
    elif safe_exp(d_output) < config.lb_ratio_output:
        alpha = np.log(config.lb_ratio_output) / d_output
    new_state.kp *= alpha
    new_state.ki *= alpha
    new_state.kd *= alpha

    d_output = update(new_state)
    new_state.original_output = state.output * safe_exp(d_output)
    new_state.output = clip(new_state.original_output,
                            state.output * config.lb_ratio_output,
                            state.output * config.ub_ratio_output)
    return new_state


def _clip_ads_bid(
    ads: List[Ad], campaign_all_actual_df: pd.DataFrame, today: datetime.datetime
) -> List[Ad]:

    unit_period_cpc = weighted_cpc_by_df(
        campaign_all_actual_df.groupby("date").sum(numeric_only=True).sort_values("date").reset_index(),
        today - datetime.timedelta(days=1), CPC_PERIOD_DAYS)

    for ad in ads:
        upper_bid, _ = calc_bid_cpc_limit(
            tuple(ad.performances), unit_period_cpc=unit_period_cpc)
        if upper_bid is not None:
            ad.rounded_bidding_price = min(ad.last_bidding_price, upper_bid)

    return ads


def wma(lst):
    # 加重移動平均の関数定義
    weight = np.arange(len(lst)) + 1
    wma = np.sum(weight * lst) / weight.sum()
    return wma


def _initialize_p(bids: np.ndarray, values: np.ndarray, valid_ads: List[Ad]) -> float:
    # １週間分の広告費の加重移動平均を算出
    lst_wma7_ad_costs = []
    for ad in valid_ads:
        lst_costs = []
        target_performances = sorted(
            ad.performances, key=lambda x: x.date)[-7:]
        for performance in target_performances:
            lst_costs.append(performance.costs)
        lst_wma7_ad_costs.append(wma(lst_costs))

    array_wma7_ad_costs = np.array(lst_wma7_ad_costs)

    p = (
        np.power(array_wma7_ad_costs, 2) * np.power(values, 2)
    ).sum() / (bids * values * np.power(array_wma7_ad_costs, 2)).sum()
    return p


def _initialize_p_and_q(
        bids: np.ndarray, values: np.ndarray, C: float,
        valid_ads: List[Ad], N: int = 1000) -> Tuple[float, float]:
    """p, qを初期化する関数．過去のデータに適合しつつlogp, logqの
    変化に対する入札額の変化が同程度になるようなp, qを選ぶ"""

    # １週間分の広告費の加重移動平均を算出
    lst_wma7_ad_costs = []
    for ad in valid_ads:
        lst_costs = []
        target_performances = sorted(ad.performances, key=lambda x: x.date)[-7:]
        for performance in target_performances:
            lst_costs.append(performance.costs)
        lst_wma7_ad_costs.append(wma(lst_costs))
    array_wma7_ad_costs = np.array(lst_wma7_ad_costs)

    #  tの値を算出。最適化問題の解を計算（一意に解ける）
    t = (bids * values * np.power(array_wma7_ad_costs, 2)).sum() \
        / (np.power(array_wma7_ad_costs, 2) * np.power(values, 2)).sum()

    # p, qの初期値を算出
    t = max(t, C * (1 + 1e-6))
    p = 1 / (2 * t)
    q = 1 / (2 * (t - C))

    return p, q


def _init_pq(
    settings: Settings,
    valid_ads: List[Ad],
    campaign_all_actual_df: pd.DataFrame,
    today: datetime.datetime,
    target_cost: float,
    obs_cost: float,
    target_kpi: float,
    obs_kpi: float,
    config: PIDConfig,
) -> Tuple[State, Optional[State]]:

    def tune_pid_params(
        new_state: State,
        init_error: float,
        config: PIDConfig,
    ) -> State:
        p_term = new_state.kp * init_error
        i_term = new_state.ki * init_error
        d_term = new_state.kd * init_error
        d_output = config.sign * (p_term + i_term + d_term)

        beta = 1
        if d_output > 0:
            beta = np.log(config.ub_ratio_output) / d_output
        elif d_output < 0:
            beta = np.log(config.lb_ratio_output) / d_output

        new_state.kp *= beta
        new_state.ki *= beta
        new_state.kd *= beta

        return new_state

    valid_ads = _clip_ads_bid(valid_ads, campaign_all_actual_df, today)

    bids = np.array([ad.rounded_bidding_price for ad in valid_ads], dtype=np.float64)
    values = np.array([ad.value(settings.purpose) for ad in valid_ads], dtype=np.float64)
    if np.sum(values) == 0 or np.sum(bids * values) == 0:
        raise ValueError("Cannot initialize p, q")
    if settings.kpi is KPI.NULL:
        # pのみの場合
        p_yesterday = _initialize_p(bids, values, valid_ads)
        new_p_state = State(
            output=p_yesterday,
            sum_error=0.0,
            error=0.0,
        )
        p_error = target_cost - obs_cost
        new_p_state = tune_pid_params(new_p_state, p_error, config)
        return new_p_state, None
    else:
        p, q = _initialize_p_and_q(bids, values, settings.C, valid_ads)
        new_p_state = State(
            output=p,
            sum_error=0.0,
            error=0.0,
        )
        p_error = target_cost - obs_cost
        new_p_state = tune_pid_params(new_p_state, p_error, config)
        new_q_state = State(
            output=q,
            sum_error=0.0,
            error=0.0,
        )
        q_error = target_kpi - obs_kpi
        new_q_state = tune_pid_params(new_q_state, q_error, config)
        return new_p_state, new_q_state


def _unit_settings(df):
    df = df.sort_values(["unit_id", "date"])
    unit_info = df.groupby('unit_id').agg({
        'mode': 'last',
        'target_kpi': 'last',
        'purpose': 'last',
        'target_kpi_value': 'last',
        'base_target_cost': 'last',
        'target_cost': 'last',
        'not_ml_applied_days': 'last',
        'p': 'last',
        'p_error': 'last',
        'p_sum_error': 'last',
        'p_kp': 'last',
        'p_ki': 'last',
        'p_kd': 'last',
        'q': 'last',
        'q_error': 'last',
        'q_sum_error': 'last',
        'q_kp': 'last',
        'q_ki': 'last',
        'q_kd': 'last',
        'yesterday_target_kpi': 'last',
        'unit_ex_observed_C': 'last',
    }).reset_index().to_dict(orient='records')

    settings = dict2settings(unit_info[0])

    return unit_info, settings


def _calc_states(
    df: pd.DataFrame,
    campaign_all_actual_df: pd.DataFrame,
    today: datetime.datetime,
    pid_config: Optional[PIDConfig] = None
) -> Tuple[Settings, State, Optional[State], bool, bool]:

    unit_info, settings = _unit_settings(df)
    p_state, q_state = dict2qpstates(unit_info[0])

    ads = []
    for (ad_type, ad_id), ad_df in df.groupby(['ad_type', 'ad_id']):
        ads.append(Ad(ad_df[ad_df['date'] < today], ad_type, ad_id, ad_df[ad_df['date'] == today]))

    daily_actual_df = df[df['date'] < today].groupby('date').agg({
        'impressions': 'sum',
        'clicks': 'sum',
        'conversions': 'sum',
        'sales': 'sum',
        'costs': 'sum',
        'bidding_price': 'mean',
        'cpc': 'mean',
        'cvr': 'mean',
        'rpc': 'mean',
    }).reset_index().to_dict(orient='records')
    historical_performances = list(map(dict2perf, daily_actual_df))
    lastday_performance = historical_performances[-1]

    unit_ex_observed_C = unit_info[0]["unit_ex_observed_C"]
    obs_kpi = settings.obs_kpi(historical_performances[-1])
    obs_cost = historical_performances[-1].costs

    valid_ads = [ad for ad in ads if
                 ad.is_valid(settings.purpose) and
                 ad.has_predicted_kpi() and
                 ad.is_enabled_bidding_auto_adjustment]

    valid_ads_num = len(valid_ads)

    if valid_ads_num == 0:
        logger.warning("[pid] valid_ads is zero.")
        return p_state, q_state, False, False, obs_kpi, valid_ads_num

    assert len(ads) > 0, \
        'length of ads must be greater than 1'
    if settings.kpi is not KPI.NULL and q_state is None:
        raise ValueError('q_state is needed for kpi constraints')

    if pid_config is None:
        pid_config = PIDConfig()

    target_cost = settings.target_cost

    is_pid_initialized = False
    if _is_init_pq(settings, p_state, q_state, pid_config.not_ml_applied_days_threshold):
        p, q = _init_pq(
            settings, valid_ads, campaign_all_actual_df, today, target_cost, obs_cost,
            settings.C, obs_kpi, pid_config)
        is_pid_initialized = True
        p_state = p
        if q_state is not None:
            q_state = q

    if isnan(lastday_performance.bidding_price):
        logger.warning('missing bidding_price')
        return None, None, False, False, obs_kpi, valid_ads_num

    is_begin_of_month = (today.day == 1)

    if settings.kpi is KPI.NULL:
        p_state = _pid_step(
            pid_config, p_state, target_cost, obs_cost, is_begin_of_month)

        return p_state, q_state, True, is_pid_initialized, obs_kpi, valid_ads_num

    assert settings.C is not None, 'target kpi must be specified for kpi constraints'

    # 月初日
    if is_begin_of_month:
        # PIDのerrorが0になるように、実績値を目標値としてp,　q更新のステップを走らせる
        p_state = _pid_step(pid_config, p_state, target_cost, target_cost, is_begin_of_month)
        q_state = _pid_step(pid_config, q_state, settings.C, settings.C, is_begin_of_month)
    else:
        assert unit_ex_observed_C is not None, 'unit_ex_observed_C is not calculated'

        # 同一の月内でweightの和をとる
        sum_weight = sum(
            [settings.calc_weight(p) for p in historical_performances if p.date.month == today.month])
        weight = settings.calc_weight(lastday_performance)
        sum_weight += weight

        # 超過ペースの場合
        if settings.target_cost < settings.base_target_cost:
            if unit_ex_observed_C != 0 and unit_ex_observed_C <= settings.C:
                # KPI目標値を下げて広告費用を抑える
                target_C = settings.target_cost / settings.base_target_cost * unit_ex_observed_C
            else:
                target_C = settings.C
            p_state = _pid_step(pid_config, p_state, target_cost, obs_cost, is_begin_of_month)
            q_state = _pid_step(pid_config, q_state, target_C, obs_kpi, is_begin_of_month, weight, sum_weight)

        # ショートペースの場合
        else:
            # KPI達成している場合、または指標がROASで28日間spaが0の(salesが28日間発生していない)
            # 場合はp,qを両方更新する
            if (
                (unit_ex_observed_C < settings.C and obs_kpi < settings.C * LATEST_KPI_BOUND_BUFF)
                or (settings.kpi == "ROAS" and daily_actual_df["sales"].sum() == 0)
            ):
                p_state = _pid_step(pid_config, p_state, target_cost, obs_cost, is_begin_of_month)
                q_state = _pid_step(pid_config, q_state, settings.C, obs_kpi, is_begin_of_month, weight, sum_weight)
            # 上記以外の場合
            else:
                if settings.mode is Mode.BUDGET:
                    # 予算モードの場合には広告費用のパラメータのみ調整
                    p_state = _pid_step(pid_config, p_state, target_cost, obs_cost, is_begin_of_month)
                elif settings.mode is Mode.KPI:
                    # kpiモードの時はkpiのパラメータのみ調整
                    q_state = _pid_step(pid_config, q_state, settings.C, obs_kpi, is_begin_of_month, weight, sum_weight)

    if p_state.original_output is None:
        p_state.original_output = p_state.output
    if q_state.original_output is None:
        q_state.original_output = q_state.output

    return p_state, q_state, True, is_pid_initialized, obs_kpi, valid_ads_num


def _reupdate_states(_p_state, _q_state, C):
    if _q_state is None:
        return _p_state, _q_state

    if pd.isnull(_p_state.output) or pd.isnull(_q_state.output):
        return _p_state, _q_state

    p_state = copy.deepcopy(_p_state)
    q_state = copy.deepcopy(_q_state)

    p = p_state.output
    q = q_state.output
    t = max((1 + q * C) / (p + q), C * (1 + 1e-6))
    p_state.output = 1 / (2 * t)
    q_state.output = 1 / (2 * (t - C))

    return p_state, q_state


def calc_states(
    df: pd.DataFrame,
    campaign_all_actual_df: pd.DataFrame,
    today: datetime.datetime,
    pid_config: Optional[PIDConfig] = None
) -> Tuple[Settings, State, Optional[State], bool, bool]:
    '''
    p, qの初期化および更新を行う
    historical_performancesは，月内の全てのパフォーマンスを含むこと
    '''

    _, settings = _unit_settings(df)

    p_state, q_state, is_updated, is_pid_initialized, obs_kpi, valid_ads_num = _calc_states(
        df, campaign_all_actual_df, today, pid_config)

    pre_reupdate_p_state = p_state
    pre_reupdate_q_state = q_state

    if valid_ads_num > 0:
        p_state, q_state = _reupdate_states(p_state, q_state, settings.C)

    return (
        settings,
        pre_reupdate_p_state,
        pre_reupdate_q_state,
        p_state,
        q_state,
        is_updated,
        is_pid_initialized,
        obs_kpi,
        valid_ads_num
    )
