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
具体的なロジックは以下参照：
[XX]

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
:widths: 1, 10, 1, 10, 1

目的, KPI制約条件, v_i, C, 最適入札額
Click最大化, なし(予算のみ), 1 / CPC_i, \-, bid_i = 1 / CPC_i / p
```

<!-- Click最大化, なし(予算のみ), 1 / CPC_i, \-, bid_i = 1 / CPC_i / p"
Click最大化, CPC, 1 / CPC_i, CPC, bid_i = 1 / CPC_i / (p + q) + CPC * q \n / CPC_i / (p + q) Conversion最大化, なし(予算のみ), CVR_i / CPC_i, \-, bid_i = CVR_i / CPC_i / p
Conversion最大化, CPA, CVR_i / CPC_i, CPA, bid_i = CVR_i / CPC_i / (p + q) + CPA * CVR *  q / (p + q) / CPC_i
売り上げ最大化, なし(予算のみ), CVR_i * SPA_i / CPC_i, \-, bid_i =  CVR_i * SPA_i / CPC_i / p
売り上げ最大化, ROAS, CVR_i * SPA_i / CPC_i, 1/ROAS, bid_i =  CVR_i * SPA_i / CPC_i / (p + q) +  1 / ROAS * q * CVR_i * SPA_i / CPC_i / (p + q) -->

<!-- ```{eval-rst}
.. csv-table::
   :header-rows: 1
   :widths: 10, 10, 1, 10, 10

   目的, KPI制約条件, v_i, C, 最適入札額
   Click最大化, なし(予算のみ), 1 / CPC_i, \-, "| bid_i = 1   /
   | CPC_i / p"
   Click最大化, CPC, 1 / CPC_i, CPC, bid_i = 1 / CPC_i / (p + q) + CPC * q \n / CPC_i / (p + q)
   Conversion最大化, なし(予算のみ), CVR_i / CPC_i, \-, bid_i = CVR_i / CPC_i / p
   Conversion最大化, CPA, CVR_i / CPC_i, CPA, bid_i = CVR_i / CPC_i / (p + q) + CPA * CVR *  q / (p + q) / CPC_i
   売り上げ最大化, なし(予算のみ), CVR_i * SPA_i / CPC_i, \-, bid_i =  CVR_i * SPA_i / CPC_i / p
   売り上げ最大化, ROAS, CVR_i * SPA_i / CPC_i, 1/ROAS, bid_i =  CVR_i * SPA_i / CPC_i / (p + q) +  1 / ROAS * q * CVR_i * SPA_i / CPC_i / (p + q)
``` -->

### 最適入札戦略の直感的な解釈
pは広告費用に関わるパラメータ，qはKPIに関わるパラメータである。各パラメータの制御と入札額の変化は以下の通り。
