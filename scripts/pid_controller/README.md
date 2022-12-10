# update month-to-date
当月のPID制御の状態を更新する際には， `update_month_to_date.py` を用いる。

前提は，boto3がインストールされたPython実行環境とする。

以下の方法で実行可能。
```bash
python update_month_to_date.py --stage {stage} --date {today}
```

- stageは，staging, preproduction, productionの３つから選択
- todayのフォーマットは，`yyyy-mm-dd`
- 実行されるパラメータだけを確認したい場合は， `--dry-run` フラグをつける

例｜2021-05-15にstagingで実行するパラメータを確認したい場合
```bash
python update_month_to_date.py --stage staging --date 2021-05-15 --dry-run
```
