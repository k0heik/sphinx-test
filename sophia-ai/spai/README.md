
## Examples of `spai.models`

### basic usage
```python
import numpy as np
from spai.models import CTRModel

X = np.random.randn(100, 10)
y = np.random.randn(100)

params = {
  'iterations': 100,
  'verbose': 0
}

model = CTRModel(params)
model.fit(X, y)
pred = model.predict(X)
```

### hyper-parameter tuning by optuna
```python
import numpy as np
from spai.models import CTRModel, get_tuned_params

X = np.random.randn(100, 10)
y = np.random.randn(100)

best_params = get_tuned_params(CTRModel, X, y, num_trials=10)

model = CTRModel(best_params)
model.fit(X, y)
pred = model.predict(X)
```

### categorical features
You need to use `pd.DataFrame` to use categorical features since `np.ndarray` can't mix `int` and `float`.
```python
import pandas as pd
import numpy as np
from spai.models import CTRModel

X_cat = pd.DataFrame(
   np.random.randint(low=0, high=10, size=(100, 1)))
X_num = pd.DataFrame(np.random.randn(100, 9), columns=list(range(1, 10)))
X = pd.concat([X_cat, X_num], axis=1)
y = pd.Series(np.random.randn(100))
 
params = {
  'iterations': 10,
  'verbose': 0,
  'cat_features': [0],  # list of indices of categorical columns
}

model = CTRModel(params)
model.fit(X, y)
pred = model.predict(X)
```

### tuning w/ categorical features
```python
import pandas as pd
import numpy as np
from spai.models import CTRModel, get_tuned_params

X_cat = pd.DataFrame(
   np.random.randint(low=0, high=10, size=(100, 1)))
X_num = pd.DataFrame(np.random.randn(100, 9), columns=list(range(1, 10)))
X = pd.concat([X_cat, X_num], axis=1)
y = pd.Series(np.random.randn(100))

best_params = get_tuned_params(CTRModel, X, y, num_trials=10,
                                cat_features=[0])  # pass here

model = CTRModel(best_params)
model.fit(X, y)
pred = model.predict(X)
```

---

## PID制御

### 入力

1. PID制御に必要なパラメータ類`（kp, ki, kd, error, sum_error, output）`をpとqについて（計6 x 2 = 12個のスカラー）
1. 日別のperformance（２８日分）
1. KPI制約の指定（`NULL, CPC, CPA, ROAS`）及び制約の上限値/下限値（aka target_kpi_value, ROASのみ下限で他は上限）
1. target_cost及びbase_target_cost
1. 最大化する指標（`CLICK, CONVERSION, SALES`）
1. モードの指定（`予算優先・KPI優先`）


### 出力

- 更新した入力 1. のパラメータ類（p, qも含む１２個のスカラー）



### 使用例
```python
from spai.optim.pid import update_states
from spai.optim.models import Settings, State, KPI, Purpose, Mode, Performance

# given
## pid controller's states from BigQuery
p_state = State(output=0.3, sum_error=-120.1, error=-30.5,
                kp=0.1, ki=0.01, kd=1e-6)
q_state = State(output=0.3, sum_error=-120.1, error=-30.5,
                kp=0.1, ki=0.01, kd=1e-6)    
## records of 28 days
historical_performances = [
    Performance(impressions=1000, clicks=100, conversions=10, sales=1000, costs=5000, bidding_price=50, ctr=0.02, cvr=0.1, rpc=10.0),
    # ...
    Performance(impressions=1000, clicks=100, conversions=10, sales=1000, costs=5000, bidding_price=50, ctr=0.02, cvr=0.1, rpc=10.0),
]

## today's performance (for estimated values of ctr, cvr and rpc)
current_performance = Performance(impressions=1000, clicks=100, conversions=10,
                        sales=1000, costs=5000, bidding_price=50, ctr=0.02, cvr=0.1, rpc=10.0)
## kpi, purpose, mode, target_cost, target_kpi and base_target_cost
settings = Settings(kpi=KPI.ROAS,  # KPI.NULL, KPI.CPC, KPI.CPA, KPI.ROAS
                    purpose=Purpose.CLICK,  # Purpose.CLICK, Purpose.CONVERSION, Purpose.SALES
                    mode=Mode.BUDGET,  # Mode.BUDGET, Mode.KPI
                    base_target_cost=10000,
                    target_cost=8000,
                    target_kpi_value=300,    
            )
# end given


p_state, q_state = update_states(settings, current_performance, historical_performances, p_state, q_state)

# store p_state and q_state to BigQuery
# and load them 1 day later
historical_performances.append(
    current_performance,
)
current_performance = Performance(impressions=1000, clicks=100, conversions=10,
                        sales=1000, costs=5000, bidding_price=50, ctr=0.02, cvr=0.1, rpc=10.0)

p_state, q_state = update_states(settings, current_performance, historical_performances, p_state, q_state)
```

---

## 入札額調整

### 入力

1. p, q
1. 日別のperformance（２８日分）
1. KPI制約の指定（`NULL, CPC, CPA, ROAS`）及び制約の上限値/下限値（aka target_kpi_value, ROASのみ下限で他は上限）
1. target_cost及びbase_target_cost
1. 最大化する指標（`CLICK, CONVERSION, SALES`）
1. モードの指定（`予算優先・KPI優先`）


### 出力

- 最適入札額

### 使用例


```python
from spai.bid_optimization import calc_bidding_price
from spai.optim.models import Settings, State, KPI, Purpose, Mode, Performance

# given
## values of p and q from BigQuery
p, q = (1.0, 1.0) 

## records of 28 days
historical_performances = [
    Performance(impressions=1000, clicks=100, conversions=10, sales=1000, costs=5000, bidding_price=50, ctr=0.02, cvr=0.1, rpc=10.0),
    # ...
    Performance(impressions=1000, clicks=100, conversions=10, sales=1000, costs=5000, bidding_price=50, ctr=0.02, cvr=0.1, rpc=10.0),
]

## today's performance (for estimated values of ctr, cvr and rpc)
current_performance = Performance(impressions=1000, clicks=100, conversions=10,
                        sales=1000, costs=5000, bidding_price=50, ctr=0.02, cvr=0.1, rpc=10.0)

## kpi, purpose, mode, target_cost, target_kpi and base_target_cost
settings = Settings(kpi=KPI.ROAS,  # KPI.NULL, KPI.CPC, KPI.CPA, KPI.ROAS
                    purpose=Purpose.CLICK,  # Purpose.CLICK, Purpose.CONVERSION, Purpose.SALES
                    mode=Mode.BUDGET,  # Mode.BUDGET, Mode.KPI
                    base_target_cost=10000,
                    target_cost=8000,
                    target_kpi_value=300,    
            )
# end given


bid = calc_bidding_price(settings, current_performance, historical_performances, p, q)
```