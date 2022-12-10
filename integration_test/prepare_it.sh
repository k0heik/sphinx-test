set -e

# データの準備
inv reset-dataset && inv create-all-tables && inv prepare-test-data &
# 各コンテナのビルド
inv prepare-containers &
wait
# target_unit.csvのアップロード
inv prepare-target-unit
