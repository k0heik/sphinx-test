set -e

# 学習パイプライン実行
date=2022-10-16
PIDS=()
inv run-batch cpc_prediction "python main.py train ${date}" &
PIDS+=($!)
inv run-batch cvr_prediction "python main.py train ${date}" &
PIDS+=($!)
inv run-batch spa_prediction "python main.py train ${date}" &
PIDS+=($!)
echo "PIDS: ${PIDS[@]}"
for PID in ${PIDS[@]}; do
  wait ${PID}
  if [ $? != 0 ]; then
    echo "Error Occurredin PID: ${PID}"
    exit 1
  fi
done

# Main処理の実行
inv run-main

# Record to bq の実行
inv run-record-to-bq

# ML_OUTPUT_BUCKETの結果を確認
inv check-output
