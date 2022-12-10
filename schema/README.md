# BigQueryのテーブル作成

invokeのコマンドにしている。

## 準備

- Google Cloud SDK のインストール

https://cloud.google.com/sdk/docs/install?hl=ja

- ライブラリのインストール

```
pip install -r requirements.txt
```

- GCP クレデンシャルの設定

```
export GOOGLE_APPLICATION_CREDENTIALS=<gcp_credential_file_path>
```

## 実行

schema ディレクトリに移動し、invokeタスクを実行する

```
# テーブルの作成
inv create-table -d <データセット名> -t <テーブル名> -p <パーティションキーフィールド名>
```

```
# テーブルの更新
inv update-table-schema -d <データセット名> -t <テーブル名>
```


## テーブルの初期化

以下のコマンドで、データウェアハウスに入札額調整機械学習に必要なテーブル作成を一括で行う

```
sh init_table.sh
```