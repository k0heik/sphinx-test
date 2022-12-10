from typing import Optional, List
import numpy as np
import pandas as pd
import math


def safe_exp(x):
    return math.exp(clip(x, lower=None, upper=50))


def clip(
        x: float,
        lower: Optional[float] = None,
        upper: Optional[float] = None) -> float:
    '''
    Clip the value of x to [lower, upper]

    Parameters
    ----------
    x : float
        A value to be clipped
    lower, upper : float or None
        Minimum and maximum value.
    '''
    if lower is not None and x < lower:
        x = lower
    if upper is not None and x > upper:
        x = upper
    return x


def weighted_ma_weight(window_size: int) -> np.ndarray:
    weights = np.zeros(window_size)
    for t in range(window_size):
        if t < 2:
            weights[window_size - t - 1] = 1.0
        else:
            weights[window_size - t -
                    1] = (window_size - t) / (window_size - 1)
    return weights


def weighted_ma(x: np.ndarray, window_size: int) -> np.ndarray:
    '''
    Calculate weighted moving average of x with given window_size.

    Parameters
    ----------
    x : np.ndarray
        A 1d array of values
    window_size : int
        The size of window to calculate moving average

    Notes
    -----
    min_periods = 1

    Returns
    -------
    output : np.ndarray
    '''
    assert len(x.shape) == 1, 'multi-dimensional array is not supported'
    x = pd.Series(x.astype(float))
    weights = weighted_ma_weight(window_size)

    output = np.zeros_like(x)
    for i in range(len(x)):
        bx = x.iloc[max(0, i - window_size + 1):i + 1]
        output[i] = np.average(bx, weights=weights[-len(bx):])

    return output


def nan2none(x):
    return x if not pd.isnull(x) else None


def nan2zero(x):
    return x if not pd.isnull(x) else 0.0


def nan2const(x, c):
    return x if not pd.isnull(x) else c


def none2const(x, c):
    return x if not pd.isnull(x) else c


def none2zero(x):
    return none2const(x, 0)


def _avoidzero(att_name, p, alt_p=None, default_value=0.0):
    if getattr(p, att_name) != 0:
        return getattr(p, att_name)
    elif alt_p is not None and getattr(alt_p, att_name) != 0:
        return getattr(alt_p, att_name)

    return default_value


def ctr(p) -> float:
    return p.clicks / p.impressions if p.impressions > 0 else 0.0


def cvr(p) -> float:
    return p.conversions / p.clicks if p.clicks > 0 else 0.0


def rpc(p) -> float:
    return p.sales / p.clicks if p.clicks > 0 else 0.0


def cpc(p, alt_denom_p=None, zero_denom=0.0) -> float:
    clicks = _avoidzero("clicks", p, alt_denom_p, zero_denom)

    return p.costs / clicks if clicks > 0 else 0.0


def weighted_cpc(ws: int, clicks: np.ndarray, costs: np.ndarray):
    ma_clicks = weighted_ma(clicks, ws)[-1]
    ma_costs = weighted_ma(costs, ws)[-1]
    if ma_clicks > 0:
        cpc = ma_costs / ma_clicks
    else:
        cpc = 0.0
    return cpc


def cpa(p, alt_denom_p=None, zero_denom=0.0) -> float:
    conversions = _avoidzero("conversions", p, alt_denom_p, zero_denom)

    return p.costs / conversions if conversions > 0 else 0.0


def inv_roas(p) -> float:
    return p.costs / p.sales if p.sales > 0 else 0.0


def roas(p, alt_denom_p=None, zero_denom=0.0, alt_num_p=None) -> float:
    costs = _avoidzero("costs", p, alt_denom_p, zero_denom)
    sales = _avoidzero("sales", p, alt_num_p, 0.0)

    return sales / costs if costs > 0 else 0.0


def safe_div(x, y):
    x = np.nan_to_num(x.astype(np.float64), posinf=0, neginf=0)
    y = np.nan_to_num(y.astype(np.float64), posinf=0, neginf=0)
    return np.divide(x, y, out=np.zeros_like(x), where=y != 0)


def ema(seq: List[Optional[float]], span: int = 7) -> float:
    s = pd.Series(seq)
    return list(s.ewm(span=span).mean())


def weighted_cpc_by_df(
    daily_actual_df: pd.DataFrame,
    target_date: np.datetime64,
    ws: int,
) -> float:
    tmp_df = pd.merge(
        daily_actual_df.sort_values("date"),
        pd.DataFrame({
            "weight": weighted_ma_weight(ws),
            "date": pd.date_range(end=target_date, freq="D", periods=ws)
        }),
        how="left", on="date"
    )
    tmp_df = tmp_df[~tmp_df["weight"].isnull()]
    tmp_df["weighted_costs"] = tmp_df["costs"].fillna(0.0) * tmp_df["weight"]
    tmp_df["weighted_clicks"] = tmp_df["clicks"].fillna(0.0) * tmp_df["weight"]
    sum_seq = tmp_df[["weighted_costs", "weighted_clicks"]].sum()

    return safe_div(sum_seq.at["weighted_costs"], sum_seq.at["weighted_clicks"])
