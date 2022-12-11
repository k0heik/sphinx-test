# 入札額調整

## 背景
Google, Yahooの検索広告やAmazonのスポンサープロダクト広告の運用では，広告やキーワードに紐づく，入札額を調整することで，予算消化をまもりつつCPCなどのKPI制約を守った広告運用が求められている．しかし，対象となる広告数は非常に多く，過去の実績データに応じて日々調整しなければならないため，人手による運用はコストが高い．そこで，これまでの取り組みでは，入札額調整のロジックを構築することで，日々の入札額調整の自動化に取り組んできた．しかし，ロジックベースのアルゴリズムは，あくまでルールベースなので，入札額の理論的な最適性を保証できていない．このため，過去の実績データからトレンドを見つけ出したりと，過去データに基づくナレッジを将来の入札額調整に活かすことが難しい課題がある．

そこで，より最適な入札戦略を構築するために，ロジックの機械学習化を進めたい．

## 前提条件
- CTR, CVR, SPAは広告ごとに推定する．PID制御の制御信号p, qはユニット単位(ポートフォリオ or 広告アカウント) ごとに計算する
- 機械学習の入札額調整では入札額をキーワードや、商品ターゲティング、オートターゲティングのように、広告単位に計算する。広告グループの入札額は計算しない。
- 入札額調整の目標及び，目標KPIの組み合わせは以下のみを想定。（※ただし以下はC-Flowの制約で技術的には他の組み合わせも可能）
  - 目標SALES & 目標KPI：ROAS
  - 目標Conversion & 目標KPI: CPA
  - 目標Click & 目標KPI: CPC
  - 上記以外の組み合わせ，たとえば，CPC最大化を目指しつつ，ROASをKPI目標として置くような運用は考えない。
- 入札額調整対象となっている広告は、c-flowから送られてくるinput jsonに含まれている広告を対象とする。

## 入札額調整の機械学習化方針
具体的なロジックは{download}`こちら <./ref/OR学会2022春季大会_川上.pdf>`を参照

入札額調整では以下の式に従って、入札額を調整する。

```{math}
bid_i = v_i / (p + q) + q * v_i * C / (p+q)
```

ただし、$v_i$は広告の価値、CはCPC, CPA, ROASなどの効率目標、p, qは予算や効率の進捗に応じて変化させる制御パラメータである。

すなわち、本入札戦略では、広告価値が高い広告の入札額をあげ、低い広告の入札額を下げるというシンプルな戦略に基づいている。

なお、v_iやCは、最適化の目的や制約条件によって変化する。対応表は以下の表を参照。

### 目的関数、制約条件に対する評価指標の対応表

```{csv-table}
:header-rows: 1
:widths: 2, 3, 4, 2, 10

目的, KPI制約条件, v_i, C, 最適入札額
Click最大化, なし(予算のみ), 1 / CPC_i, \-, bid_i = 1 / CPC_i / p
Click最大化, CPC, 1 / CPC_i, CPC, bid_i = 1 / CPC_i / (p + q) + CPC * q \n / CPC_i / (p + q)
Conversion最大化, なし(予算のみ), CVR_i / CPC_i, \-, bid_i = CVR_i / CPC_i / p
Conversion最大化, CPA, CVR_i / CPC_i, CPA, bid_i = CVR_i / CPC_i / (p + q) + CPA * CVR *  q / (p + q) / CPC_i
売り上げ最大化, なし(予算のみ), CVR_i * SPA_i / CPC_i, \-, bid_i =  CVR_i * SPA_i / CPC_i / p
売り上げ最大化, ROAS, CVR_i * SPA_i / CPC_i, 1/ROAS, bid_i =  CVR_i * SPA_i / CPC_i / (p + q) +  1 / ROAS * q * CVR_i * SPA_i / CPC_i / (p + q)
```

### 最適入札戦略の直感的な解釈
pは広告費用に関わるパラメータ，qはKPIに関わるパラメータである。各パラメータの制御と入札額の変化は以下の通り。

```{csv-table}
:header-rows: 1
:widths: 1, 5, 5

, 大きくなる, 小さくなる
p, 入札額が下がるので広告費用が少なくなる, 入札額が上がり広告費用が使われるようになる
q, 入札額が下がり，効率が良くなる, 入札額が上がり，効率が悪くなる
```

- 上記式をそれぞれの目的関数及びKPI制約条件に当てはめると以下の表で表すことができる．
- 最適入札額のp, qは実績データが全て揃うまで真の値が判明しない変数なので，実績データをもとに逐次更新する必要あり．
- 以下の要件定義書では，CTR, CVRなどのパラメータを決定する方法及び，そのパラメータをもとにp, qの値をpid制御で逐次更新する手順を記載する．

## 入札額調整の流れ
<br>

```{figure} ./img/bid_flow.svg
:alt: figure
:align: center
:scale: 100
```

## CVR, SPA, CPCの推定
CVR,SPAの予測結果を読み出す
- 推定モデルは，{doc}`../model/cvr_model`, {doc}`../model/spa_model`,{doc}`../model/cpc_model`の通り

## 全体日予算計算
{doc}`../budget/total_budget`を参照.

## 残予算の判定
```{csv-table}
:header-rows: 1
:widths: 3, 5, 3

プロセス名, 実施内容, 補足
"残予算がない場合には入札額及びp, qは更新しない", "残予算がない場合には入札額及びp, qは更新せずに、入札額調整を終了する。 <br> ※ 日予算が自動的に最低日予算にセットされるため、予算超過は抑えられる想定", "このケースで最適化が終了した場合においても”ML適用あり”と判定する"
```

## PID制御
予算目標、効率目標及び、予算・効率の達成状況に応じて、入札額を決定するハイパーパラメータp, qを制御する.

<br>

```{figure} ./img/pid_flow.svg
:alt: figure
:align: center
:scale: 100
```

### パラメータの初期化に利用するadの抽出
PID制御のパラメータを初期化するために利用する広告を抽出する。
- 目的click: click>0
- 目的cv : click>0 & cv>0
- 目的sales: click>0 & sales>0

```{code-block}
:linenos:

"""
変数一覧
ad.v : v_i 広告の価値
purpose: ユニットの目的
"""

valid_ads = []
for ad in ads:
    def is_valid():
        # 目的に応じて有効性の判定
        if purpose is Purpose.CLICK:
            ad,weekly_clicks() > 0
        elif purpose is Purpose.CONVERSION:
            return (
                ad.weekly_clicks() > 0 and
                ad.monthly_conversion() > 0
            )
        elif purpose is Purpose.SALES:
            return (
                ad.weekly_clicks() > 0 and
                ad.monthly_sales() > 0
            )
    # 機械学習の予測結果が存在しない場合は有効な広告から外す
    def has_predicted_kpi():
        if purpose is Purpose.CLICK:
            return ad.predicted_cpc is not None:
        elif purpose is Purpose.CONVERSION:
            return (
                ad.predicted_cpc is not None and
                ad.predicted_cvr is not None
            )
        elif purpose is Purpose.SALES:
            return (
                ad.predicted_cpc is not None and
                ad.predicted_cvr is not None and
                ad.predicted_spa is not None
            )
    # click, cv, salesが有効な値以上、かつ、kpi予測値が存在し、かつ、入札額調整が有効になっている広告のみ追加する
    if is_valid() & has_predicted_kpi() & ad.is_enabled_bidding_auto_adjustment is True:
        valid_ads.append(ad)
```

### 制御信号p, qの初期値設定
pの初期値が設定されていない場合，ルールベースが前日に走っていた場合，もしくは前日とKPI制約条件が異なる場合、ユニットに設定されている入札額の平均値に入札額が近くなるようにpの初期値を設定する

```{note}
ml_apply=Trueの条件：

MLで算出した入札額と、実際に適用された入札額が50%以上一致していること

初期値の計算方法は[こちら](https://negocia.atlassian.net/wiki/spaces/SPAI/pages/1786151308)を参照
```



```{code-block}
:linenos:

"""
変数一覧
ad.v : v_i 広告の価値
target_cost: 今日の1日の予算
target_kpi: 目標kpi(cpc, cpa, 1/roas)
observed_kpi_yesterday: 昨日のkpiの実績値
clicks_yesterday: 前日のclicks
conversions_yesterday: 前日のconversions
sales_yesterday: 前日のsales
costs_yesterday: 前日のcosts
valid_ads: 有効な広告の集合
"""

if 運用初日or 過去3日間ml_applyがFalse or 前日とKPI制約条件が異なる:
    # 強制的にリセットする
    p_q_init_flag = True

# observed_kpi_yesterdayの算出
if target = 'cpc':
    observed_kpi_yesterday = cost_yesterday/ clicks_yesterday if clicks_yesterday > 0 else 0
elif target = 'cpa':
    observed_kpi_yesterday = cost_yesterday / conversions_yesterday if conversions_yesterday > 0 else 0
elif target = 'roas':
  　# roasと逆なので注意
    observed_kpi_yesterday = cost_yesterday / sales_yesterday if sales_yesterday > else 0

# 初期化を実行する
if p_q_init_flag is True:
    p_state, q_state = initialize_pid_parameters()
    # kp, ki, kdのチューニング
    p_error = target_cost - costs_yesterday
    p_state = tune_pid_params(p_state, p_error)
    if target_kpi is not None:
        q_error = target_kpi - observed_kpi_yesterday　
        q_state = tune_pid_params(q_state, q_error)
else:
    p_state = 直近のp_state
    q_state = 直近のq_state



def initialize_pid_parameters():
    ## 広告の価値を設定
    for ad in valid_ads:
      if ad.bidding_Mode == BiddingMode.bidding:
        # 足切りされていない場合はvalueの表に従ってvalue設定
        if target_name == PromotionPurpose.CLICKS:
            value = 1 / predicted_cpc
        elif target_name == PromotionPurpose.CONVERSIONS:
            value = predicted_cvr / predicted_cpc
        else:
            value = predicted_cvr * predicted_spa / predicted_cpc
        ad.v = value

    # 入札額の上限値および下限値制約を適用して，ルールベースで異常な値に設定されていたとしても正常に動作するように補正する
    for ad in valid_ads:
        ad.rounding_bidding_price = round_up_and_down(ad.bidding_price) # このラウンドアップ＆ダウンは，入札額調整最後に実施するラウンドアップ，ダウンと同様
    # p, q初期化処理
    if target_name = NULL:　
        # 効率目標がない場合はpの身の初期値を決定する
        ad_weekly_costs = weighted_ma(ad.costs, window_size = 7)
        ad_weekly_clicks = weighted_ma(ad.costs, window_size = 7)
        ad_ema_weekly_cpc = ad_weekly_costs / ad_weekly_clicks
        p_yesterday = np.sum(ad.v ** 2 for ad in valid_ads) / (np.sum(ad.rounding_bidding_price * ad.v) for ad in valid_ads))
    # pのpid制御のエラーもリセットする
    else:
        # 効率目標がある場合にはp , qを両方初期化する
        initialize_p_and_q([ad.rounding_bidding_price for ad in ads], [ad.v for ad in ads] , C, valid_ads)

def initialize_p_and_q(bids: np.ndarray, values: np.ndarray, C: float, valid_ads: List[Ad], N: int = 1000) -> Tuple[float, float]:
    """p, qを初期化する関数．過去のデータに適合しつつlogp, logqの
    変化に対する入札額の変化が同程度になるようなp, qを選ぶ"""

    # 加重移動平均の関数定義
    def wma(lst):
        weight = np.arange(len(lst)) + 1
        wma = np.sum(weight * lst) / weight.sum()
        return wma

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
    t = (bids * values * np.power(array_wma7_ad_costs, 2)).sum() / (np.power(array_wma7_ad_costs, 2) * np.power(values, 2)).sum()

    # 一意に決定
    t = max(t, C * (1 + 1e-6))
    p = 1 / (2 * t)
    q = 1 / (2 * (t - C))
    return p, q

def tune_pid_params(state, error):
    new_state = replace(state)
    p_term = new_state.kp * error
    i_term = new_state.ki * error
    d_term = new_state.kd * error
    d_output = -1 * (p_term + i_term + d_term)

    beta = 1
    if d_output > 0:
        beta = np.log(1.5) / d_output
    elif d_output < 0:
        beta = np.log(1/1.5) / d_output
    else:
        beta = 1

    new_state.kp *= beta
    new_state.ki *= beta
    new_state.kd *= beta

    return new_state

class Ad():
    is_valid
```

#### 例外処理
- 以下の事象に該当する場合は、PIDとBIDの処理をスキップし、「処理不能だった」として結果を出力する。その際、出力される入札額は前日と同じ値をoutput jsonに出力する。
  - adに含まれるbidding_priceが全てNoneの場合
  - p, qの初期化に失敗した場合
  - p, qが0の場合

### モードを切り替えて入札額の最適化

```{code-block}
:linenos:

"""
base_target_cost: 1日あたりの理想的な広告消化スピード(全体予算/稼働日数)
target_cost: 今日の1日の予算
target_kpi: 目標kpi(cpc, cpa, 1/roas)
unit_ex_observed_C: 月次の実績KPI(CPC, ROAS, CPA)
observed_kpi_yesterday: 昨日のkpiの実績値
clicks_in_this_month: 月初からのclicksの合計
conversions_in_this_month: 月初からのconversionsの合計
sales_in_this_month: 月初からのsalesの合計
costs_in_this_month: 月初からのcostsの合計
clicks_yesterday: 前日のclicks
conversions_yesterday: 前日のconversions
sales_yesterday: 前日のsales
costs_yesterday: 前日のcosts
conversions_in_28_days: ２８日間のconversionsの合計
sales_in_28_days: 28日間のsalesの合計
mean_spa_in_28_days: 28日間のspaの平均
"""

# unit_ex_observed_Cの算出
if target = 'cpc':
    unit_ex_observed_C = cost_in_this_month / clicks_in_this_month if clicks_in_this_month > 0 else cost_in_this_month
elif target = 'cpa':
    unit_ex_observed_C = cost_in_this_month / conversions_in_this_month if conversions_in_this_month > 0 else cost_in_this_month
elif target = 'roas':
    # roasと逆なので注意
    mean_spa_in_28_days = sales_in_28_days / conversions_in_28_days if conversions_in_28_days > 0 else 0
    if sales_in_this_month > 0:
        unit_ex_observed_C = cost_in_this_month / sales_in_this_month
    elif mean_spa_in_28_days > 0:
        unit_ex_observed_C = cost_in_this_month / mean_spa_in_28_days
    else:
        unit_ex_observed_C = cost_in_this_month

# observed_kpi_yesterdayの算出
if target = 'cpc':
    observed_kpi_yesterday = cost_yesterday/ clicks_yesterday if clicks_yesterday > 0 else 0
elif target = 'cpa':
    observed_kpi_yesterday = cost_yesterday / conversions_yesterday if conversions_yesterday > 0 else 0
elif target = 'roas':
  　# roasと逆なので注意
    observed_kpi_yesterday = cost_yesterday / sales_yesterday if sales_yesterday > else 0


#  月初以外はp, q更新
if 月初:
    # p, qは更新しない．
    # PIDのerrorが0になるように、実績値を目標値としてp,　q更新のステップを走らせる
    p = pid_cost(target_cost, target_cost,...)
    q = pid_kpi(target_kpi, target_kpi,...)
else:
    # 超過ペースの場合
    if target_cost < base_target_cost:
        if unit_ex_observed_C <= target_kpi:
            # KPI目標値を下げて広告費用を抑える
            target_kpi = target_cost / base_target_cost * unit_ex_observed_C
        p = pid_cost(target_cost, yesterday_cost,...)
        q = pid_kpi(target_kpi, observed_kpi_yesterday,...)
    # ショートペースの場合
    else:
        # KPI達成している場合、または指標がROASで28日間spaが0の(salesが28日間発生していない)
        # 場合はp,qを両方更新する
        if (target == "roas" and mean_spa_in_28_days == 0) or
            (unit_ex_observed_C <= target_kpi and observed_kpi_yesterday <= target_kpi * 1.5):
            p = pid_cost(target_cost, costs_yesterday,...)
            q = pid_kpi(target_kpi, observed_kpi_yesterday,...)
        else:
            if mode = 'budget':
                # 予算モードの場合には広告費用のパラメータのみ調整
                p = pid_cost(target_cost, costs_yesterday,...)
            elif mode = 'kpi'
                # kpiモードの時はkpiのパラメータのみ調整
                q = pid_kpi(target_kpi, observed_kpi_yesterday,...)
    p, q = reupdate_states(p, q, target_kpi)

```

### 広告費用の制御信号pの更新
pid_cost関数の定義

```{code-block}
:linenos:

"""
変数一覧
p: 広告費用の制御パラメータp
target_cost: 今日の日予算
e: 今日の日予算と昨日の広告費用の差分
e_before: 昨日の日予算と一昨日の広告費用の差分
sum_e: 差分の累積値
kp: 広告費用のPID制御の比例項
ki: 広告費用のPID制御の積分項
kd: 広告費用のPID制御の微分項
yeseterday_cost: 昨日の広告費用
"""

pid_cost(target_cost, yesterday_cost,...):
    # 初期値設定
    kp= kp or 0.1
    ki= ki or 0.01
    kd= kd or 1e-6

    # 今日の予算と昨日の広告費用の合計の差分を計算
    e = target_cost - yesterday_cost

    # 誤差の累積値を計算．ただし，月初/期初の場合は誤差の累積値をリセット
    sum_e = sum_e + e　if today != 月初 else e

    # 誤差の差分を計算．ただし，月初/期初の場合は誤差の差分は0
    delta_e = e - e_before if e_before or today != 月初 else 0

    # もし誤差の方向が変化した場合(ショートからオーバーペース or オーバーペースからショートになった場合)
    if e * e_before < 0:
        # 誤差の累積をリセット
        sum_e = 0
        # 目標値と昨日の広告費用との差分が5%以上大きい(変化しすぎている)場合は，制御の大きさを弱める
        if abs(e)   > target_cost * 0.05:
            kp = kp * 0.8
            ki = ki * 0.8
            kd = kd * 0.8

    # 積分項が日予算の5倍以上離れている場合は制御の大きさを強める
    if abs(sum_e) > target_cost * 5:
        kp = kp * 1.2
        ki = ki * 1.2
        kd = kd * 1.2

    # 学習率を動的に調整して更新を1/1.5 ~ 1.5倍以内に収める
    alpha = 1.0
    if np.exp(-(kp * e + ki * sum_e + kd * delta_e)) > 1.5:
        alpha = - log(1.5) / (kp * e + ki * sum_e + kd * delta_e)
    elif np.exp(-(kp * e + ki * sum_e + kd * delta_e)) < 1/1.5:
        alpha = - log(1/1.5) / (kp * e + ki * sum_e + kd * delta_e)

    kp = kp * alpha
    ki = ki * alpha
    kd = kd * alpha

    # pの更新
    p_after = p * np.exp(-(kp * e + ki * sum_e + kd * delta_e))
```

### kpiの制御信号qの更新
pid_kpi関数の定義

```{code-block}
:linenos:

"""
変数一覧
q: kpiの制御用パラメータq
target_kpi: 目標kpi(cpc, cpa, 1/roas)
e: 目標kpiと昨日の実績kpiの差分
e_before: 昨日の目標kpiと一昨日の実績kpiの差分
sum_e: 差分の累積値
kp: kpiのPID制御の比例項
ki: kpiのPID制御の積分項
kd: kpiのPID制御の微分項
observed_kpi_yesterday: 昨日のkpiの実績値
yesterday_cpc: 昨日のcpcの実績値
yesterday_cpa: 昨日のcpaの実績値
yesterday_roas: 昨日のroasの実績値
"""

pid_kpi(target_kpi, observed_kpi_yesterday,...):
    # 初期値設定
    kp= kp or 0.1
    ki= ki or 0.01
    kd= kd or 1e-6

    # 誤差を算出
    e = target_kpi - observed_kpi_yesterday

    # 誤差の累積値を計算．ただし，月初/期初の場合は，誤差を0にリセットする
    sum_e = sum_e + e if today != 月初/期初 else e

    # 誤差の差分を計算．ただし，月初/期初の場合は微分誤差は0とする
    delta_e = e - e_before if e_before or today != 月初/期初 else 0

    # もし誤差の方向が変化した場合(ショートからオーバーペース or オーバーペースからショートになった場合)
    if e * e_before < 0:
        # 誤差の累積をリセット
        sum_e = 0

        # 目標値と昨日の実績値との差分が5%以上大きい(変化しすぎている)場合は，制御の大きさを弱める
        if abs(e)   > target_kpi * 0.05:
            kp = kp * 0.8
            ki = ki * 0.8
            kd = kd * 0.8

    # 積分項が目標KPIの5倍以上離れている場合は制御の大きさを強める
    if abs(sum_e) > target_kpi * 5:
        kp = kp * 1.2
        ki = ki * 1.2
        kd = kd * 1.2

    # 学習率を動的に調整して更新を1/1.5 ~ 1.5倍以内に収める
    alpha = 1.0
    if np.exp(- (kp * e + ki * sum_e + kd * delta_e)) > 1.5:
        alpha = - log(1.5) / (kp * e + ki * sum_e + kd * delta_e)
    elif np.exp(-(kp * e + ki * sum_e + kd * delta_e)) < 1/1.5:
        alpha = - log(1/1.5) / (kp * e + ki * sum_e + kd * delta_e)

    kp = kp * alpha
    ki = ki * alpha
    kd = kd * alpha

    # qの更新
    q_after = q * np.exp(-(kp * e + ki * sum_e + kd * delta_e))
```

#### 例外処理
- 以下の事象に該当する場合は、PIDとBIDの処理をスキップし、「処理不能だった」として結果を出力する。その際、出力される入札額は前日と同じ値をoutput jsonに出力する。
  - adに含まれるbidding_priceが全てNoneの場合
  - qの初期化に失敗した場合
  - qが0の場合

### 制御信号p, qの再更新
reupdate_states(p, q, target_kpi)の定義．
p, qの入札額への影響を等しくなるように再更新する処理．

```{code-block}
:linenos:

def reupdate_states(p, q, target_kpi):
    if p is not None:
        t = max((1 + q * target_kpi) / (p + q), target_kpi * (1 + 1e-6))
        p_state.output = 1 / (2 * t)
        q_state.output = 1 / (2 * (t - target_kpi))

    return p, q
```

## 入札額の計算
k決定したp, qに応じて入札額を計算する．


### 足切りの実施
clickが存在しない広告は最適化のしようがないので、入札をルールベースで評価する。clickが存在する広告は、MLでの制御を実施する。

```{note}
optimization_settings.is_auto_apply_bid_changes = Falseであっても一旦計算はする
```

```{code-block}
:linenos:

"""
変数：
portfolio.purpose: ポートフォリオの目的
ad.weekly_clicks: 一週間の平均クリック
ad.monthly_conversions: 一ヶ月の平均コンバージョン
ad.monthly_sales: 一ヶ月の平均売り上げ
ad.ema_weekly_cpc: 1週間の移動平均CPC
"""

for ad in ads:
    # 過去一週間のクリックが0回以内の場合は足切り
    if ad.weekly_clicks > 0
        # 目的cpcの場合
        if portfolio.purpose = 'click':
            ad.bidding_mode = BiddingMode.bidding
        # 目的cpaの場合
        elif portfolio.purpose = 'conversion':
            # 過去28日間のコンバージョン数が0回の場合は足切り
            ad.is_opt_enable = ad.monthly_conversion > 0:
            ad.bidding_mode = BiddingMode.bidding
        # 目的roasの場合
        elif portfolio.purpose = 'sales':
            # 過去28日間の売り上げが0円の場合は足切り
            ad.is_opt_enable = ad.monthly_sales > 0:
            ad.bidding_mode = BiddingMode.bidding
        　else:
            # クリック0以上の場合は仮のvalueで入札額調整する
            ad.bidding_mode = BiddingMode.provisional_bidding
    # clickが0の場合
    else:
        # 最適化が無効な場合はルールで予算調整
        ad.bidding_mode = BiddingMode.rulebase
        # ショートペースの場合
        if target_cost > base_target_cost:
        # インプレッションが少なすぎる広告の入札額を強化する
        if ad.weekly_impression < 50:
            ad.bidding_price = max(ad.bidding_price * 1.1, minimum_bidding_price)
```

### 最適入札戦略にしたがって入札額の更新

```{code-block}
:linenos:

# ユニットの指数移動平均
unit_ewm_sales = ad_sum['sales'].ewm(alpha=0.2).mean().values[-1]
unit_ewm_conversions = ad_sum['conversions'].ewm(alpha=0.2).mean().values[-1]
unit_ewm_clicks = ad_sum['clicks'].ewm(alpha=0.2).mean().values[-1]
unit_ewm_cvr = unit_ewm_conversions / unit_ewm_clicks

for ad in ads:
    # 広告の最適化が無効（click = 0）な場合は何もしない
    if ad.bidding_Mode not isin [BiddingMode.bidding, BiddingMode.provisional_bidding]:
        continue
    if ad.bidding_Mode == BiddingMode.bidding:
        # 足切りされていない場合はvalueの表に従ってvalue設定
        if target_name == PromotionPurpose.CLICKS:
            value = 1 / predicted_cpc
        elif target_name == PromotionPurpose.CONVERSIONS:
            value = predicted_cvr / predicted_cpc
        else:
            value = predicted_cvr * predicted_spa / predicted_cpc
    elif ad.bidding_Mode = BiddingMode.provisional_bidding:
        # クリック0以上場合は仮想的な暫定値で入札額を設定
        sum_click = ad.monthly_clicks.sum() # 過去28日間のクリック合計
        sum_cost = ad.monthly_costs.sum() # 広告費用合計
        monthly_cpc = sum_cost / sum_click # 月の平均CPC
        if target_name == PromotionPurpose.CONVERSIONS:
            # cvrが1/クリック数だと仮定。cpcは実績を使用
            value = min(1 / sum_click, unit_ewm_cvr) / monthly_cpc
        elif target_name == PromotionPurpose.SALES:
            if unit_ewm_conversions > 0:
                # cvrが1/クリック数だと仮定。cpcは実績を使用。spaはユニット全体の値を使用。
                value = min(1 / sum_click, unit_ewm_cvr) * unit_ewm_sales / unit_ewm_conversions / monthly_cpc
            else:
                # 入札額変更しない
                continue
    ad.v = value
    if target_kpi is NULL:
        # 効率目標なしの場合、p, v, cpcにしたがって，入札額の更新
        bidding_price =  ad.v / p
    else:
        # 効率目標ありの場合、p, q, v, Cにしたがって，入札額の更新
        bidding_price =  ad.v * (1/ (p + q)  + q * C / (p + q))

    # 広告費用のweeklyの移動平均を計算
    ad_weekly_ema_costs = weighted_ma(ad.costs, window_size = 7)
    unit_weekly_ema_cost = weighted_ma(unit.costs, window_size = 7)

    # コストを一定以上費やしていて、kpi制約に違反している広告は、入札額を上げない
    if (mode == 'kpi' and observed_kpi_in_month > target_kpi)
        or (mode == 'budget' and target_cost < base_target_cost and observed_kpi_in_month > target_kpi
        and target_kpi is not null):
        if target = 'cpc':
            obserbed_ad_kpi = ad.costs_yesterday / ad.clicks_yesterday if ad.clicks_yesterday > 0 else np.inf
        elif target = 'cpa':
            obserbed_ad_kpi = ad.costs_yesterday / ad.conversions_yesterday if ad.conversions_yesterday > 0 else np.inf
        elif target = 'roas':
            obserbed_ad_kpi = ad.costs_yesterday / ad.sales_yesterday if ad.sales_yesterday > 0 else np.inf
        if ad.weekly_ema_cost > unit_weekly_ema_cost * 0.01 and obserbed_ad_kpi > target_kpi:
            # 広告費用がユニット全体の1%以上かつ、前日のkpiがtarget_kpiよりも悪化している広告は入札額を増加させない
            bidding_price = max(ad.bidding_price_yesterday, bidding_price)
    # 入札額をクリッピング制御する
    if bidding_price > ad.bidding_price * 1.2:
        ad.bidding_price = ad.bidding_price * 1.2
    elif bidding_price < ad.bidding_price * 0.8:
        ad.bidding_price = ad.bidding_price * 0.8
    else:
        ad.bidding_price = bidding_price
```

#### provisional biddingについての補足
単一のユニットに存在する広告には、実績データとしてimpressionやclickが多い広告と少ない広告が存在する。

ここで、impressionやclickが十分に存在する広告は機械学習で比較的高精度に予測が可能となる。一方、impressionやclickのデータ数が少ない広告は、精度高く広告の効果を予測することはできない。そこで、広告ごとにデータ数に応じて適用する機械学習ロジックを変更したい。

従来は、ルールベースの足切りロジックで実装されたロジックに従って、足切りを実施し、足切りされた広告は入札額調整せずルールベースで制御する機械学習ロジックとなっていた。

しかし、上記ロジックでは、足切りされた広告の入札額が変更されずに広告費用が垂れ流しとなっており、全体の効果を悪化させる事象が発生していた。そこで、従来のロジックで足切りにかかった広告の中でもclickが0以上存在する広告対しては、clicks数や広告費用に応じて入札額を調整するモードとなるようにしたい。

具体的には、例えばROAS制約の場合、

- Sales 0でもclick多く発生してsales発生していない場合は、入札額を下げる
- Salse 0でもclick少なくてSales発生していない場合は、入札額を上げる

ようにする。

### 後処理

**入札額のラウンドアップ**

最低入札額以下の場合、そこまで入札額を上げる。

**入札額のラウンドダウン**

最高入札額以上の場合、そこまで入札額を下げる。また，入札額はCPCの3倍以上にあげないようにする．

**小数点をround_up_point に応じて切り上げる**

input の round_up_point 以下は切り上げる。

例えば、米ドルの場合、小数点第 3 位以下は切り上げるため、round_up_point = 3 で連携される。

いままので計算の結果、入札額が 1.832 米ドルだった場合、小数点第 3 位以下は切り上げて、 1.84 になる。

```{code-block}
:linenos:

# CPCは広告費用÷クリックで計算する(CPCを直接加重平均で計算しない．clickの重みをきちんと考慮する)
unit_period_costs = weighted_ma(unit.costs, window_size = 14)
unit_period_clicks = weighted_ma(unit.costs, window_size = 14)
unit_period_cpc = unit_period_costs / unit_period_clicks

for ad in ads:
    # 入札額が最高入札額以上の場合，そこまで入札額を下げる
    if ad.bidding_price > ad.maximum_bidding_price:
        ad.bidding_price = ad.maximum_bidding_price
    # 広告の最適化が無効な場合は以降の処理は実施しない
    if ad.is_opt_enable is False:
        continue
    # CPCは広告費用÷クリックで計算する(CPCを直接加重平均で計算しない．clickの重みをきちんと考慮する)
    ad_period_costs = weighted_ma(ad.costs, window_size = 14)
    ad_period_clicks = weighted_ma(ad.costs, window_size = 14)
    ad_ema_period_cpc = ad_period_costs / ad_period_clicks
    # 広告の2週間のCPCが0超だった場合
    if ad_period_cpc > 0:
        # 広告のCPCがその広告の2週間平均CPC(emaでも良い)の3倍もしくはユニット平均の広告のCPCの3倍以上だった場合，それ以上入札額を上げても意味がないので，入札額をCPCの3倍までに抑える
        if ad.bidding_price > max(ad.ema_period_cpc * 3, unit_period_cpc * 3):
        ad.bidding_price = max(ad.ema_period_cpc * 3, unit_period_cpc * 3)
    # 広告の2週間のCPCが0だった場合
    elif unit_period_cpc > 0:
        # 広告のCPCがその広告が属するユニットの2週間平均CPC(emaでも良い)の3倍以上だった場合，それ以上入札額を上げても意味がないので，入札額をCPCの3倍までに抑える
        if ad.bidding_price > unit_period_cpc * 3:
        ad.bidding_price = unit_period_cpc * 3
    # ユニットとしてコストが発生していない場合には何もしない．
    else:
        pass
```