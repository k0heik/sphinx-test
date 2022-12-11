# ユニット単位の日予算

## 全体広告費計算
日予算計算では前日までの広告費用と当日の予算情報を元に、ユニットの残予算を計算し、残予算をユニットの残日数で割ることで、計算日当日に使うべき全体広告費用を計算する。

### 計算日前日までの広告費合計額計算
残予算を残日数で割って、全キャンペーンの計算日当日の日予算合計額を計算する

まずは計算日前日までの広告費を合計する



```{code-block}
:linenos:

"""
変数一覧
budget_type: 予算タイプ（月次：monthly or 期間：period）
campaigns: ユニットに所属しているキャンペーン郡
  - perfomances: キャンペーンの実績値
  - date: キャンペーン実績値の日付
  - cost: キャンペーンの各日の広告費
sum_of_cost_until_yesterday:
  - 期間予算の場合：期間開始日から計算日前日までの利用広告費
  - 月次予算の場合：月初から計算日前日までの利用広告費
start_date_of_this_month: 今月の初日
yesterdays_date: 計算日前日の日付
"""

# 月初から計算日前日までの広告費を合計する。
# ※全てのキャンペーンの実績値を合計する
for cp in campaigns:
    for pf in cp.perfomances:
        if pf.date >= start_date_of_this_month and pf.date <= yesterdays_date:
            sum_of_cost_until_yesterday += pf.cost
```
### 全キャンペーンの日予算を計算する
計算日当日も含めた残日数で割って、計算日当日の全キャンペーンの日予算合計額を算出する

```{code-block}
:linenos:

"""
変数一覧
unit_budget: ユニット予算
remain_unit_budget: 残予算
remain_days: 残日数
target_cost: 計算日当日のアロケーション対象の日予算合計
campaigns: ユニットに所属しているキャンペーン郡
  - perfomances: キャンペーンの実績値
  - date: キャンペーン実績値の日付
  - cost: キャンペーンの各日の広告費
sum_of_cost_until_yesterday:
  - 期間予算の場合：期間開始日から計算日前日までの利用広告費
  - 月次予算の場合：月初から計算日前日までの利用広告費
start_day: 今月の月初の日付
end_day: 今月の月末の日付
todays_date: 計算日当日の日付   
"""
 
# 残予算を計算する（予算 - 広告費)
remain_unit_budget = unit_budget - sum_of_cost_until_yesterday

# 残りのboost coefficientの合計を計算する
remaining_coefficient = df[(today() <= df.date) & (df.date <= end_day)]['coefficient'].sum()


# 今日のboost coefficientを計算する
today_coefficient = df[(df.date == today())]['coefficient'].values[0]

# 残予算を当日の係数/残係数で割って全キャンペーンの計算日当日の日予算合計を算出する
target_cost = remain_unit_budget * today_coefficient / remaining_coefficient

# 当該期間におけるboost coefficientの合計を計算する
total_coefficient = df[(start_day <= df.date) & (df.date <= end_day)]['coefficient'].sum()

# 理想的な予算消化ペースの定義
ideal_budget_for_today = promotion_budget * today_coefficient　/ total_coefficient

# 最終日を除いて補正処理を実行
if remain_days > 1:
    # 予算ショートペースの処理
    if target_cost > ideal_budget_for_today:
        # 全体日予算がideal_budgetを超えている場合は、予算消化ペースをideal_budgetへ近づけるように補正
        target_cost = 2 * target_cost - ideal_budget_for_today
    # 予算オーバーペースの処理
    else:
        # 全体日予算がideal_budgetを下回っている場合は、残予算を残日数で割ったペースで予算を消費させる
        pass



```