# MLパイプラインのITを行うためのディレクトリ

## 準備
### 前提

- docker
- docker-compose

### 環境変数
```bash
cp .env.sample .env
# Open .env and replace XXXXXX w/ your credential.
```

### IT実行用のコンテナの起動
```bash
docker-compose up -d --build
```

## ITの実行

### テスト用データセット構築/IT実行
```bash
docker-compose exec it bash exec_prepare_and_run_it.sh
```

### テスト用データセットの削除
```bash
docker-compose exec it inv delete-dataset
```

## テストケースの作成方法

まずITの流れは次のとおりである。

1. testcase.ymlからテストケースを定義する各パラメータを読み込む
1. 前項の内容をもとにテストデータを作成し登録する
1. 各サブシステムをKPI推定train処理→日次定期実行内容の順に実行する

テストケースの作成では，testcase.ymlに以下のパラメータ定義すればよい。

※testcase.ymlのdefaultに記載のあるパラメータは，個々のテストケースに未設定の場合に用いられる。
