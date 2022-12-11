# キャンペーン単位の日予算CAP

## 背景
入札単価の調整のみだと、急激な需要拡大（給料日後の月末などに起きやすい）により予算オーバーする可能性がある。そこで、日予算で cap をはめることで予算オーバーを防ぎたい。

## 前提条件
- 広告ではなく，キャンペーンごとに日予算のcapを設定する
- ルールベースでは期末3日間は日予算で制御，それ以外の日では，日予算に当たっていた場合は1.2倍する制御を行っているが，機械学習は必ずしもこの通り日予算を制御する必要はない
- portfolio全体で予算上限を設定する機能はない
- 機会損失の可能性があるので，できるだけ予算上限には当てないようにする

## 機能要件
処理時間：

計算量は少ないので，1プロモーション10秒以内を想定．もちろんできるだけ速い方が良い

処理データ量：

100 kb / portfolio * 100 portfolio程度を想定

## 実装方針
- [A New Optimization Layer for Real-Time Bidding Advertising Campaigns](https://negocia.atlassian.net/wiki/spaces/SPAI/pages/1140262546/A+New+Optimization+Layer+for+Real-Time+Bidding+Advertising+Campaigns) のbudget partitionをc-flow用にアレンジして，予算の配分比率を決定する．
- 残日数に応じて日予算制約を設定する．残日数が5日以内の場合は，厳しく設定．それ以外の日は，事故防止のために，設定日予算の五倍を上限として予算を設定

## 実装内容
<br>

```{figure} ./img/budget_cap_flow.svg
:alt: figure
:align: center
:scale: 100
```

### 入出力項目

#### 入力
- portfolio単位
  - target_cost: 今日の目標日予算
  - purpose: ユニットの目的
  - remaining_days: 残日数
- campaign単位
  - campaign_id
  - weight: 日予算を配分するための重み(昨日の値)
  - click_yesterday: 昨日のクリック数
  - conversion_yesterday: 昨日のコンバージョン数
  - sales_yesterday: 昨日のコンバージョン値
  - costs_yesterday: 昨日の広告費用

#### 出力
- campaign単位
  - campaign_id
  - weight: 日予算の重み
  - daily_budget_upper: キャンペーンの日予算上限制約

### 1. 全体日予算計算
入札額調整と同様の計算手法を適用：{doc}`./total_budget`

### 2. 残予算がない場合には、日予算を最低日予算に設定
残予算がない場合には、日予算を最低日予算に設定し、処理を終了する(weightの更新も実施しない)

```{note}
weightは前日と同じ値を踏襲して変更を加えない
```

```{code-block}
:linenos:

if target_cost <= 0:
    for cp in campaigns:
        cp.daily_budget = MINIMUM_BUDGET # 最低日予算に設定
    # 日予算capの処理を終了する
```

### 3. 直近1週間のユニットのclickが0の場合は全キャンペーンの日予算変更せずに処理を終了する

```{code-block}
:linenos:

# unitの直近1週間のcpcを計算
unit_weekly_cpc = unit_weekly_costs / unit_weekly_clicks if unit_weekly_clicks > 0 else 0

if unit_weekly_cpc == 0:
    # 日予算は前日の日予算のまま、日予算capの処理を終了する
```

### 4. 無効なキャンペーンの除外
月内では有効だったが、途中で無効になったキャンペーンなどは、実績の集計には用いるが、weightの計算には利用しない


```{code-block}
:linenos:

if (cp.status != enabled) or (cp.end_at is not null and today > cp.end_at):
    # weightの計算から除外。日予算を変更しない。
```

#### 未解決の問題
厳密には、serving_statusによって、日予算が操作できないパターンも存在する。このため、別途c-flow側から有効なcampaignを送ってもらう対応を実施する。

### 5. 有効だが実績が存在しないキャンペーンは日予算を制御しない

```{note}
emaの計算で少なくとも7日前までのデータが必要のため
```

```{code-block}
:linenos:

if 直近7日間において、キャンペーンの実績データが1レコードも存在しない:
    # weightの計算から除外。日予算を変更しない。
```

### 6. weightの初期化
もし，重みの初期値が存在しない場合は，重みの初期値を初期化する

```{code-block}
:linenos:

"""
変数一覧
campaign_ids: キャンペーンIDの集合
campaign_id: キャンペーンのID
weight[campaign_id]: あるキャンペーンの日予算割合．全キャンペーンを足すと一になる．
"""

# 重みの履歴が一切存在しない新しいキャンペーンのweight初期化
if campaignのウェイトが存在しない:
    if キャンペーンの広告費用の7日間指数移動平均 > 0 :
        # キャンペーンのの広告費用の移動平均7日間をユニットの広告費用の移動平均7日間で割ることで重みを算出する
        weight[t, c.campaign_id] = ema(costs, 7) / ema(unit_costs, 7)
    else:
        新規キャンペーンのweight = 1 /len(campaign_ids)
    # 最後にweightの正規化処理を実行
    weight = do_normalize(weight)
```

#### 例外処理
初期化後にweightが全てnullの場合はエラーで落ちる

### 7. キャンペーンの価値計算
キャンペーンの単位日予算あたりの価値を，実績KPI / (weight * 全体日予算)　で計算する．※実績KPIはclick, conversion, salesを指す

ただし，日予算制約が有効ではない状況で，かつ，(weight * 全体日予算)が広告費用を上回っていた場合，実績KPI 通りには広告の効果が得られないとみなして，実績KPIを(weight * 全体日予算) / 広告費用倍で固定する

```{note}
最後のminは，もし，日予算よりも広告費用が多かった場合は，日予算 / 広告費用に比例して，広告の価値が下がると仮定して，広告の価値を補正する．

例：

とあるキャンペーンの単位日予算あたりの広告効果

日予算(weigh * target) = 0.2 * 10000 = 2000 → 実際には適用されていない

広告費用 = 3000

コンバージョン数=30

補正後の広告費用の価値 = 2000 / 3000 * 30 = 20

単位日予算あたりのコンバージョン数は，20だと考えて予算を更新
```

```{code-block}
:linenos:

"""
変数名
campaigns: キャンペーンの集合
c: キャンペーン
t: 計算対象日の日付
b: キャンペーンの予算設定の集合
v: キャンペーンの価値の集合
value: キャンペーンの価値
costs: キャンペーンの広告費用
"""

t = today()

for c in campaigns:
    # 目的に応じて，キャンペーンの価値を設定
    if portfolio.purpose = 'click':
        # 7日間の移動平均で計算する
        value = campaign.click_ema_7days
        costs = campaign.cost__ema_7days
    elif portfolio.purpose = 'conversion':
        # 28日間の移動平均で計算する
        value = campaign.conversion_ema_28days
        costs = campaign.cost_ema_28days
    elif portfolio.purpose = 'sales':
        # 28日間の移動平均で計算する
        value = campaign.conversion_values_ema_28days
        costs = campaign.cost_ema_28days
    else:
        NotImplementedError
    # 前日の仮想的な予算を計算（設定していなくても計算する）
    b[t, c.campaign_id] = weight[c.campaign_id] * portfolio_budget_today
    # 目標予算と，広告費用の比率から広告の価値を補正
    if costs > 0:
        # 仮想的な予算よりも，広告費用の方が多くなっていたら補正する
        if costs >= b[t, c.campaign_id]:
            v[t, c.campaign_id] = b[t, c.campaign_id] / costs * value
        else:
            v[t, c.campaign_id] = value
        # 以前の仕様書min(b[t, c.campaign_id], costs)/ costs * valueと処理内容は同様．if文にしたほうがわかりやすかったので変更
    else:
        # 広告費用が0の時はclick, conversion, sales何も発生していない前提
        v[t, c.campaign_id] = 0
```

### 8. 各キャンペーンの単位日予算あたりの価値から日予算配分を変更する勾配を計算
勾配の計算は，キャンペーンの単位日予算あたりの価値が最も高くなり，かつ，weightが1 / campaign_idsから大きく離れすぎないように計算する．

イメージとしては以下の関数を最小化している

```{math}
\max(\sum{q(w) - \lambda/2 \cdot |w - u|^2})
```

```{code-block}
:linenos:

"""
変数一覧：
u: 日予算の基準で，1 / キャンペーン数で計算
q: キャンペーンの単位日予算あたり価値．これを最大化するように予算の割り振りwを決定する
max_q: キャンペーン単位日予算あたりの価値のポートフォリオ内での最大値
w: キャンペーンの予算比率. 0~1の範囲．ポートフォリを合計で1になる値
l: 勾配．この勾配方向にキャンペーンの予算比率wを調整する
lmd: 0.1を設定．L2正則化項．キャンペーンの予算比率がuから離れることに対するペナルティ
alpha: 0.1を設定．一度の更新で，予算比率を変更する量を制御するためのパラメータ
b: キャンペーンの予算設定の集合
"""

u = 1 / len(campaings)
for c in campaigns:
    # 単位広告費用あたりの価値qを計算
    q[c.campaign_id] = v[t, c.campaign_id] / b[t, c.campaign_id]

# qの最大値を計算
max_q = max(q)

# qが0の場合，重みは変更しない
if max_q == 0:
    for c in campaigns:
        w[t + 1, c.campaign_id] = w[t, c.campaign_id]
    return w

# 勾配を計算
for c in campaigns:
    # 価値qをノーマライズ
    q[c.campaign_id] = q[c.campaign_id] / max_q
    # 勾配を計算
    l[c.campaign_id] = - q[c.campaign_id] + lmd_t * (w[t, c.campaign_id] - u)
    # update rule of gradient descent
    p[c.campaign_id] = np.exp(- alpha * l[c.campaign_id])
```

### 9. 日予算の配分比率を計算
P大きさで計算対象日の日予算割合を決定する

```{code-block}
:linenos:

# qの最大値を計算
for c in campaigns:
    # 価値qをノーマライズ
    w[t + 1, c.campaign_id] = w[t, c.campaign_id] * p[c.campgin_id] / sum(p[c.campaign_id] * w[t, c.campaign_id] in c in campaings)
```

### 10. 残日数に応じて，日予算の上限値を計算
ユニット(ポートフォリオ or ad account)の予算に，キャンペーンごとに決定した日予算割合をかけて，日予算の上限を設定する．

```{note}
remaining_daysは最終日に1になる想定

日予算上限値の急激な変化を防ぐため，最大2倍までの変化を許す．

下げる側は事故リスクが少ないので，いくらでも下げてOK

最大でも日予算2倍オーバーまで強要するようにupperを調整する
```

```{code-block}
:linenos:

"""
変数一覧
remaining_days: 残日数
target_cost: ユニット(ポートフォリオ or アドアカウント)の設定日予算
potential_weight_sum: ポテンシャルがある広告の重みの合計
unit_weekly_cpc   # ユニットの１週間平均CPC
target_kpi: ユニットの設定kpi(cpc, cpa, 1 / roas)
"""

# 今日利用される広告費用の合計を見積もる
total_expected_cost = 0
potential_weight_sum = 0

for c in campaigns:
    # ブースト係数をかけた日予算を算出
    c.boost_daily_budget_yesterday = c.daily_budget_yesterday * c.daily_budget_boost_coeficient_yesterday
    c.boost_today_budget = w[t + 1, c.campaign_id] * target_cost
    # ブースト係数をかけた日予算でポテンシャル判定
    if (c.cost_yesterday < c.boost_daily_budget_yesterday * 0.8 or c.boost_daily_budget_yesterday is None) and
            c.cost_yesterday < c.boost_today_budget:
        # 昨日予算を使い切れておらず，かつ，今日の予算が，昨日の広告費よりも大きい、もしくはキャンペーンの予算が設定されていない場合は、昨日の広告費用がそのまま使われると考える
        total_expected_cost += c.cost_yesterday
        # 予算の可否を検討する
        cp.has_potential = False # 伸び代なし判定
    else:
        # それ以外の場合は，予算通りに消化されると考える
        total_expected_cost += w[t + 1, c.campaign_id] * target_cost
        cp.has_potential = True # 伸び代あり判定
        potential_weight_sum += w[t + 1, c.campaign_id] # 伸び代ありキャンペーンの重みの合計

# マージンとして利用可能な広告費用の合計を計算する
total_margin = (target_cost - total_expected_cost)
# ブーストしない場合のマージンを計算する
noboost_total_marging = total_margin / today_coefficient

# 日予算ブースト時には日予算ブーストをc-flow側で実施するので、予算を下げる
noboost_target_cost = target_cost / today_coefficient

# マージンを有効なキャンペーンに割り振り上限を設定する
for c in campaigns:
    # weightとunit日予算からcampaign日予算を計算
    budget =　w[t + 1, c.campaign_id] * noboost_target_cost
    if cp.has_potential:
        # ポテンシャルがある場合はマージンを足す。ただし、最大で2倍まで
        c.daily_budget_upper = min(2 * budget, budget + w[t + 1, c.campaign_id] * noboost_total_margin / potential_weight_sum)
    else:
        # ポテンシャルがない場合は「前日の日予算」または「過去１週間のコストの最大値の1.2倍」のうち、小さいほうを採用
        budget = min(budget, c.daily_budget_upper_yesterday, c.perfomances.cost_last_week.max() * 1.2)
        # ただし、プロモーション全体cpcの二倍または前日の日予算のうち小さい方を下限とする
        if unit_weekly_cpc > 0:
            budget = max(budget, min(unit_weekly_cpc * 2, c.daily_budget_upper_yesterday))
        c.daily_budget_upper = budget
        # weightの値も設定した日予算に応じて修正する
        w[t + 1, c.campaign_id] =  min(c.daily_budget_upper / noboost_target_cost, w[t + 1, c.campaign_id])
```

### 11. 後処理
上下限値を設定する

```{code-block}
:linenos:

# 広告費用のweeklyの移動平均を計算
c.weekly_ema_costs = weighted_ma(c.costs, window_size = 7)
unit_weekly_ema_cost = weighted_ma(unit.costs, window_size = 7)

# コストを一定以上費やしていて、kpi制約に違反しているキャンペーンは、日予算を上げない
if (mode == 'kpi' and observed_kpi_in_month > target_kpi)
        or (mode == 'budget' and target_cost < base_target_cost and observed_kpi_in_month > target_kpi
        and target_kpi is not null):
    if target == 'cpc':
        campaign_kpi = c.costs_yesterday / c.clicks_yesterday if c.clicks_yesterday > 0 else np.inf
    elif target == 'cpa':
        campaign_kpi = c.costs_yesterday / c.conversions_yesterday if c.conversions_yesterday > 0 else np.inf
    elif target == 'roas':
        campaign_kpi = c.costs_yesterday / c.sales_yesterday if c.sales_yesterday > 0 else np.inf
    if c.weekly_ema_cost > unit_weekly_ema_cost * 0.1 and campaign_kpi > target_kpi:
        # 広告費用がユニット全体の10%以上かつ、前日のkpiがtarget_kpiよりも悪化しているキャンペーンは日予算を増加させない
        c.daily_budget_upper = max(c.daily_budget_upper, c.daily_budget_upper_yesterday)

# 日予算は昨日の２倍を上限として計算
if 月初 & 月末の残予算 == 0:
    # 月初かつ、前月の残予算が0だった時は一時的に予算の上限を緩和する(事故っても、大事にならないようにideal_budgetで抑えるようにする)
    c.daily_budget_upper = min(c.daily_budget_upper, ideal_budget)
else:
    budget_upper_bound = 2 * c.daily_budget_upper_yesterday
    c.daily_budget_upper = min(c.daily_budget_upper, budget_upper_bound)

# 日予算は上下限を守るようにする
c.daily_budget_upper = min(c.daily_budget_upper, minimum_daily_budget)
c.daily_budget_upper = max(c.daily_budget_upper, minimum_daily_budget)
```

## 例外処理
- プロモーションに目的が設定されていない場合はnot implementation error

## 未解決の問題
- キャンペーンにすでに人が予算を設定している場合は，その予算設定を優先するように変更する
- 曜日による効果の変化を吸収するために過去の割引率まで考慮してKPI，広告費用をアップデートしてもいいかも(PoC)