import datetime
import pandas as pd
import numpy as np
import pytest
from spai.optim.pid import (
    PIDConfig,
    _pid_step,
    calc_states,
    _reupdate_states,
    _calc_states,
    _init_pq,
    _clip_ads_bid,
    _initialize_p,
    _initialize_p_and_q,
    _is_init_pq,
    LATEST_KPI_BOUND_BUFF,
)
from spai.optim.pid_util import KP_DEFAULT, KI_DEFAULT, KD_DEFAULT, CPC_LIMIT_RATE
from spai.optim.models import Settings, KPI, Purpose, Mode, State, Ad


@pytest.fixture
def today():
    return datetime.datetime(2021, 4, 29)


@pytest.fixture
def yesterday(today):
    return today - datetime.timedelta(days=1)


@pytest.fixture
def df(today):
    n = today.day
    return pd.DataFrame({
        'unit_id': ['a_1'] * n,
        'ad_type': ['keyword'] * n,
        'ad_id': [1] * n,
        'date': pd.date_range(end=today, periods=n),
        'mode': [1] * n,
        'target_kpi': [1] * n,
        'purpose': [1] * n,
        'target_kpi_value': [1] * n,
        'base_target_cost': [1] * n,
        'target_cost': [1] * n,
        'is_enabled_bidding_auto_adjustment': [True] * n,
        'p': [1] * n,
        'p_error': [1] * n,
        'p_sum_error': [1] * n,
        'p_kp': [1] * n,
        'p_ki': [1] * n,
        'p_kd': [1] * n,
        'q': [1] * n,
        'q_error': [1] * n,
        'q_sum_error': [1] * n,
        'q_kp': [1] * n,
        'q_ki': [1] * n,
        'q_kd': [1] * n,
        'impressions': [1000] * n,
        'clicks': [100] * n,
        'conversions': [10] * n,
        'sales': [1000] * n,
        'costs': [5000] * n,
        'bidding_price': [50] * n,
        'ctr': [0.1] * n,
        'cpc': [50] * n,
        'cvr': [0.1] * n,
        'rpc': [10.0] * n,
        'yesterday_target_kpi': [1] * n,
        'not_ml_applied_days': [0] * n,
        'unit_ex_observed_C': [0.1] * n,
    })


@pytest.fixture
def campaign_all_actual_df(yesterday):
    n = yesterday.day
    return pd.DataFrame({
        'campaign_id': [82] * n,
        'date': pd.date_range(end=yesterday, periods=n),
        'impressions': [1000] * n,
        'clicks': [100] * n,
        'conversions': [10] * n,
        'sales': [1000] * n,
        'costs': [5000] * n,
    })


@pytest.fixture
def invalid_df(today):
    n = today.day
    return pd.DataFrame({
        'unit_id': ['a_1'] * n,
        'ad_type': ['keyword'] * n,
        'ad_id': [2] * n,
        'date': pd.date_range(end=today, periods=n),
        'mode': [1] * n,
        'target_kpi': [1] * n,
        'purpose': [1] * n,
        'target_kpi_value': [1] * n,
        'base_target_cost': [1] * n,
        'target_cost': [1] * n,
        'is_enabled_bidding_auto_adjustment': [True] * n,
        'p': [1] * n,
        'p_error': [1] * n,
        'p_sum_error': [1] * n,
        'p_kp': [1] * n,
        'p_ki': [1] * n,
        'p_kd': [1] * n,
        'q': [1] * n,
        'q_error': [1] * n,
        'q_sum_error': [1] * n,
        'q_kp': [1] * n,
        'q_ki': [1] * n,
        'q_kd': [1] * n,
        'impressions': [10] * n,
        'clicks': [0] * n,
        'conversions': [0] * n,
        'sales': [0] * n,
        'costs': [5000] * n,
        'bidding_price': [50] * n,
        'ctr': [0.001] * n,
        'cpc': [50] * n,
        'cvr': [0.001] * n,
        'rpc': [3.0] * n,
    })


@pytest.fixture
def default_state():
    return State(
        output=1,
        sum_error=1,
        error=1,
        kp=1,
        ki=1,
        kd=1
    )


def test_pid_step():
    config = PIDConfig()
    state = State(1.0)
    _pid_step(config, state, 1.0, 0.0)


@pytest.mark.parametrize("error_output", [None, np.nan, pd.NA])
def test_pid_step_assert_error(error_output):
    config = PIDConfig()
    state = State(1.0)

    state.output = error_output

    with pytest.raises(AssertionError):
        _pid_step(config, state, 1.0, 0.0)


def test_calc_states(df, campaign_all_actual_df, today):
    ret = calc_states(df, campaign_all_actual_df, today)

    assert len(ret) == 9


@pytest.mark.parametrize('only_invalid_df', [True, False])
@pytest.mark.parametrize('not_ml_applied_days', [0, 3])
@pytest.mark.parametrize('kpi', [None, 'CPC', 'CPA', 'ROAS'])
@pytest.mark.parametrize('purpose', ['CLICK', 'CONVERSION', 'SALES'])
@pytest.mark.parametrize('mode', ['BUDGET', 'KPI'])
@pytest.mark.parametrize('base_target_cost', [80, 120])
def test_config_pid(
    df, invalid_df, campaign_all_actual_df, today,
    default_state, only_invalid_df, not_ml_applied_days,
    kpi, purpose, mode, base_target_cost
):
    # 前日までの履歴しかないadを足す
    tmp_df = df.iloc[-2:, :].copy()
    tmp_df["ad_type"] = "keyword"
    tmp_df["ad_id"] = 3
    df = pd.concat([df, tmp_df]).reset_index()

    if only_invalid_df:
        df = invalid_df

    df['not_ml_applied_days'] = not_ml_applied_days

    df['mode'] = mode
    df['target_kpi'] = kpi
    df['purpose'] = purpose
    df['base_target_cost'] = base_target_cost
    df['target_kpi_value'] = 3000
    df['yesterday_target_kpi'] = kpi
    df['unit_ex_observed_C'] = 0.1

    p_state, q_state, is_updated, is_inited_pq, obs_kpi, valid_ads_num = _calc_states(
        df, campaign_all_actual_df, today)
    df['target_cost'] = 100

    if kpi is None:
        assert obs_kpi is None
    else:
        assert obs_kpi is not None

    if only_invalid_df:
        assert not is_updated
        assert not is_inited_pq
        if kpi is not None:
            assert p_state.output == default_state.output
            assert p_state.sum_error == default_state.sum_error
            assert p_state.error == default_state.error
            assert p_state.kp == default_state.kp
            assert p_state.ki == default_state.ki
            assert p_state.kd == default_state.kd
            assert q_state.output == default_state.output
            assert q_state.sum_error == default_state.sum_error
            assert q_state.error == default_state.error
            assert q_state.kp == default_state.kp
            assert q_state.ki == default_state.ki
            assert q_state.kd == default_state.kd
        else:
            assert p_state.output == default_state.output
            assert p_state.sum_error == default_state.sum_error
            assert p_state.error == default_state.error
            assert p_state.kp == default_state.kp
            assert p_state.ki == default_state.ki
            assert p_state.kd == default_state.kd
    else:
        assert is_updated
        assert is_inited_pq is (not_ml_applied_days >= PIDConfig().not_ml_applied_days_threshold)
        if kpi is not None:
            assert (p_state is not None and q_state is not None)
        else:
            assert (p_state is not None)


@pytest.mark.parametrize('mode', ["KPI", "BUDGET"])
@pytest.mark.parametrize('kpi', ["CPC", "CPA", "ROAS"])
@pytest.mark.parametrize('is_kpi_reached', [True, False])
def test_calc_states_reached_unit_ex_observed_C(
    df, campaign_all_actual_df, today,
    mode, kpi, is_kpi_reached
):
    # 当日の値しかないadを足す
    tmp_df = df.copy()
    tmp_df['ad_type'] = "keyword"
    tmp_df['ad_id'] = 3
    df = pd.concat([df, tmp_df], axis=0, ignore_index=True)

    target_kpi_value = 200
    df['not_ml_applied_days'] = 0
    df['mode'] = mode
    df['target_kpi'] = kpi
    df['purpose'] = ""
    df['target_cost'] = 100
    df['base_target_cost'] = df['target_cost']
    df['target_kpi_value'] = target_kpi_value
    df['yesterday_target_kpi'] = kpi

    # unit_ex_observed_C以外の値は共通（KPI達成状態）
    if kpi == "ROAS":
        df["sales"] = target_kpi_value * df["costs"] + 1
    else:
        df["costs"] = target_kpi_value - 1

    if is_kpi_reached:
        if kpi == "ROAS":
            df["unit_ex_observed_C"] = 1 / (target_kpi_value + 1)
        else:
            df["unit_ex_observed_C"] = target_kpi_value - 1
    else:
        if kpi == "ROAS":
            df["unit_ex_observed_C"] = 1 / (target_kpi_value - 1)
        else:
            df['unit_ex_observed_C'] = target_kpi_value + 1

    df["p_kp"] = KP_DEFAULT
    df["p_ki"] = KI_DEFAULT
    df["p_kd"] = KD_DEFAULT
    df["q_kp"] = KP_DEFAULT
    df["q_ki"] = KI_DEFAULT
    df["q_kd"] = KD_DEFAULT

    p_state, q_state, is_updated, is_inited_pq, obs_kpi, valid_ads_num = _calc_states(
        df, campaign_all_actual_df, today)

    assert is_updated and not is_inited_pq
    assert obs_kpi is not None

    if is_kpi_reached:
        assert p_state.output != 1
        assert q_state.output != 1
    else:
        if mode == "KPI":
            assert p_state.output == 1
            assert q_state.output != 1
        else:
            assert p_state.output != 1
            assert q_state.output == 1


@pytest.mark.parametrize('mode', ['BUDGET', 'KPI'])
@pytest.mark.parametrize('kpi', ["CPC", "CPA", "ROAS"])
@pytest.mark.parametrize('is_kpi_reached', [True, False])
def test_calc_states_reached_obs_kpi(
    df, campaign_all_actual_df, today,
    mode, kpi, is_kpi_reached
):
    # 当日の値しかないadを足す
    tmp_df = df.copy()
    tmp_df['ad_type'] = "keyword"
    tmp_df['ad_id'] = 3
    df = pd.concat([df, tmp_df], axis=0, ignore_index=True)

    target_kpi_value = 3000
    df['not_ml_applied_days'] = 0
    df['mode'] = mode
    df['target_kpi'] = kpi
    df['purpose'] = ""
    df['target_cost'] = 100
    df['base_target_cost'] = df['target_cost']
    df['target_kpi_value'] = target_kpi_value
    df['yesterday_target_kpi'] = kpi

    df["clicks"] = 1
    df["conversions"] = 1
    df["sales"] = 1
    df["costs"] = 1

    # obs_kpiの計算に使用しない値は共通（KPI達成状態）
    if kpi == "ROAS":
        df['unit_ex_observed_C'] = 1 / (target_kpi_value + 1)
    else:
        df['unit_ex_observed_C'] = target_kpi_value - 1

    yesterday = today - datetime.timedelta(days=1)
    if is_kpi_reached:
        if kpi == "ROAS":
            df.loc[df["date"] == yesterday, "sales"] = target_kpi_value / LATEST_KPI_BOUND_BUFF + 1
        else:
            df.loc[df["date"] == yesterday, "costs"] = target_kpi_value * LATEST_KPI_BOUND_BUFF - 1
    else:
        if kpi == "ROAS":
            df.loc[df["date"] == yesterday, "sales"] = target_kpi_value / LATEST_KPI_BOUND_BUFF
        else:
            df.loc[df["date"] == yesterday, "costs"] = target_kpi_value * LATEST_KPI_BOUND_BUFF

    df["p_kp"] = KP_DEFAULT
    df["p_ki"] = KI_DEFAULT
    df["p_kd"] = KD_DEFAULT
    df["q_kp"] = KP_DEFAULT
    df["q_ki"] = KI_DEFAULT
    df["q_kd"] = KD_DEFAULT

    p_state, q_state, is_updated, is_inited_pq, obs_kpi, valid_ads_num = _calc_states(
        df, campaign_all_actual_df, today)

    assert is_updated and not is_inited_pq
    assert obs_kpi is not None
    if is_kpi_reached:
        assert p_state.output != 1
        assert q_state.output != 1
    else:
        if mode == "KPI":
            assert p_state.output == 1
            assert q_state.output != 1
        else:
            assert p_state.output != 1
            assert q_state.output == 1


@pytest.mark.parametrize("is_q_is_none", [True, False])
def test_reupdate_states(is_q_is_none):
    p_state = State(output=1)
    q_state = State(output=2)
    C = 0.1

    if is_q_is_none:
        q_state = None

    new_p_state, new_q_state = _reupdate_states(p_state, q_state, C)

    if is_q_is_none:
        assert new_p_state.output == p_state.output
        assert new_q_state is None
    else:
        assert new_p_state.output != p_state.output
        assert new_q_state.output != q_state.output


def test_bom_sum_error():
    config = PIDConfig()
    state = State(1.0)
    state.sum_error = 30.0
    state.error = 5.0
    state = _pid_step(config, state, 1.0, 0.0, True)
    assert state.sum_error == pytest.approx(state.error)


@pytest.mark.parametrize(
    'weight, sum_weight',
    [
        (0, -1),
        (5, 3),
        (-1, 1),
    ]
)
def test_invalid_weight(weight, sum_weight):
    config = PIDConfig()
    state = State(1.0)
    with pytest.raises(ValueError):
        _pid_step(
            config, state, 1.0, 0.0, weight=weight, sum_weight=sum_weight)


def test_adaptive_kp_ki_and_kd():
    config = PIDConfig()
    # lower bound case
    state = State(1.0)
    state = _pid_step(config, state, target=1e4, observed=0)
    assert state.original_output == pytest.approx(config.lb_ratio_output)
    # upper bound case
    state = State(1.0)
    state = _pid_step(config, state, target=0, observed=1e4)
    assert state.original_output == pytest.approx(config.ub_ratio_output)


def test_init_pq(df, campaign_all_actual_df, today):
    def get_s(kpi, purpose):
        settings = Settings(kpi, purpose, Mode.BUDGET, 10, 10, kpi, False, 10000)
        return settings

    ads = []
    for (ad_type, ad_id), ad_df in df.groupby(['ad_type', 'ad_id']):
        ads.append(Ad(ad_df[ad_df['date'] < today], ad_type, ad_id, ad_df[ad_df['date'] == today]))

    config = PIDConfig()
    # 手計算との一致確認
    p, q = _init_pq(get_s(KPI.NULL, Purpose.CLICK), ads, campaign_all_actual_df, today, 1, 1, 1, 1, config)
    assert p.output == pytest.approx((1 / 50) ** 2 / ((1 / 50) * 50))
    assert p.error == 0
    assert p.sum_error == 0
    assert q is None

    p, q = _init_pq(get_s(KPI.NULL, Purpose.CONVERSION), ads, campaign_all_actual_df, today, 1, 1, 1, 1, config)
    assert p.output == pytest.approx(((1 / 50) * 0.1) ** 2 / ((1 / 50) * 0.1 * 50))
    assert p.error == 0
    assert p.sum_error == 0
    assert q is None

    p, q = _init_pq(get_s(KPI.NULL, Purpose.SALES), ads, campaign_all_actual_df, today, 1, 1, 1, 1, config)
    assert p.error == 0
    assert p.sum_error == 0
    assert q is None

    p, q = _init_pq(get_s(KPI.ROAS, Purpose.SALES), ads, campaign_all_actual_df, today, 1, 1, 1, 1, config)
    assert p.error == 0
    assert p.sum_error == 0
    assert q.error == 0
    assert q.sum_error == 0

    p, q = _init_pq(get_s(KPI.CPC, Purpose.SALES), ads, campaign_all_actual_df, today, 1, 1, 1, 1, config)
    assert p.error == 0
    assert p.sum_error == 0
    assert q.error == 0
    assert q.sum_error == 0


def test_init_pq_clip_bid(df, campaign_all_actual_df, today):
    """上限制約が有効かを確認するテスト.大きすぎる入札額に対して，pqが変化しないことを確認する"""
    settings = Settings(KPI.ROAS, Purpose.SALES, Mode.BUDGET, 10, 10, KPI.ROAS, False, 10000)
    config = PIDConfig()

    def get_pq(df, bid):
        df = df.copy()
        df["bidding_price"] = bid
        ads = []
        for (ad_type, ad_id), ad_df in df.groupby(['ad_type', 'ad_id']):
            ads.append(Ad(ad_df[ad_df['date'] < today], ad_type, ad_id, ad_df[ad_df['date'] == today]))
        p, q = _init_pq(settings, ads, campaign_all_actual_df, today, 1, 1, 1, 1, config)
        return p.output, q.output

    assert get_pq(df, 1_000) != get_pq(df, 10)
    assert get_pq(df, 10_000) == get_pq(df, 100_000)


def test_clip_ads_bid(df, campaign_all_actual_df, today):
    df["bidding_price"] = 10_000

    ads = list()
    cpcs = list()
    for (ad_type, ad_id), ad_df in df.groupby(['ad_type', 'ad_id']):
        ads.append(Ad(ad_df[ad_df['date'] < today], ad_type, ad_id, ad_df[ad_df['date'] == today]))
        cpcs.append(ad_df["costs"].iloc[-1] / ad_df["clicks"].iloc[-1])

    ads = _clip_ads_bid(ads, campaign_all_actual_df, today)
    for ad, cpc in zip(ads, cpcs):
        assert ad.rounded_bidding_price != 10_000
        assert ad.rounded_bidding_price == cpc * CPC_LIMIT_RATE


def test_initialize_p_optimality(df, today):
    """最適性テスト"""
    n = 30
    rtol = 1e-10

    bids = np.exp(np.random.randn(n))
    values = np.random.rand(n)
    valid_ads = [Ad(df[df['date'] < today], 'keyword', 1, df[df['date'] == today])]

    def loss(p):
        bid_hat = values / p
        return np.power(bids - bid_hat, 2).mean()

    p = _initialize_p(bids, values, valid_ads)
    eps = p * 1e-6
    for i in range(10):
        d = eps * 2 ** i
        assert loss(p) < loss(p + d) * (1 + rtol)
        assert loss(p) < loss(p - d) * (1 + rtol)


@pytest.mark.parametrize("C", [0, 1e-4, 1e-3, 1e-2, 1e-1, 1.0, 1e1, 1e2, 1e3, 1e4])
def test_initialize_p_and_q(df, C, today):
    n = 100
    rtol = 1e-8
    valid_ads = [Ad(df[df['date'] < today], 'keyword', 1, df[df['date'] == today])]

    bids = np.exp(np.random.randn(n))
    values = np.random.rand(n)

    def loss(p, q):
        bid_hat = values / (p + q) * (1 + q * C)
        return np.power(bids - bid_hat, 2).mean()

    p, q = _initialize_p_and_q(bids, values, C, valid_ads)
    eps = p * 1e-6
    for i in range(10):
        d = eps * 2 ** i
        assert loss(p, q) < loss(p + d, q) * (1 + rtol)
        assert loss(p, q) < loss(p - d, q) * (1 + rtol)
        assert loss(p, q) < loss(p, q + d) * (1 + rtol)
        assert loss(p, q) < loss(p, q - d) * (1 + rtol)
        assert loss(p, q) < loss(p + d, q + d) * (1 + rtol)
        assert loss(p, q) < loss(p + d, q - d) * (1 + rtol)
        assert loss(p, q) < loss(p - d, q + d) * (1 + rtol)
        assert loss(p, q) < loss(p - d, q - d) * (1 + rtol)


@pytest.mark.parametrize("kpi", ["cpc", "cvr", "rpc"])
def test_ad_without_kpi(df, kpi, today):
    ad = Ad(df[df['date'] < today], 'keyword', 1, df[df['date'] == today])
    assert ad.has_predicted_kpi()
    df[kpi] = None
    ad = Ad(df[df['date'] < today], 'keyword', 1, df[df['date'] == today])
    assert not ad.has_predicted_kpi()


@pytest.mark.parametrize(
    'not_ml_applied_days, p_value, q_value, kpi, yesterday_kpi, expected',
    [
        (3, 1, 1, KPI.NULL, KPI.NULL, True),
        (0, None, 1, KPI.NULL, KPI.NULL, True),
        (0, 1, None, KPI.ROAS, KPI.ROAS, True),
        (0, 1, None, KPI.NULL, KPI.NULL, False),
        (0, 1, 1, KPI.ROAS, KPI.ROAS, False),
        (0, 1, None, KPI.NULL, KPI.ROAS, True),
        (0, 1, 1, KPI.ROAS, KPI.CPC, True),
    ]
)
def test_is_init(not_ml_applied_days, p_value, q_value, kpi, yesterday_kpi, expected):
    settings = Settings(
        kpi,
        Purpose.CLICK,
        Mode.BUDGET,
        0,
        0,
        yesterday_kpi,
        not_ml_applied_days=not_ml_applied_days
    )
    p = State(p_value)
    q = State(q_value)

    result = _is_init_pq(settings, p, q)
    assert result == expected
