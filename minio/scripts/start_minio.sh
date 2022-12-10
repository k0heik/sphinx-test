#!/bin/bash

minio server --console-address :${MINIO_WEB_PORT} ${MINIO_DATA_DIR} &

# minioの起動待ち（すぐに起動するが念のため）
sleep 1

mc alias set minio http://localhost:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}

# バケット作成時の重複エラーを無視
set +e

# buckets witten in .env.sample
mc mb minio/optimise-bidding-ml-preproduction
mc mb minio/optimise-bidding-ml-staging
mc mb minio/optimise-bidding-ml-production

# buckets for IT
mc mb minio/optimise-bidding-ml-develop
mc mb minio/optimise-bidding-ml-output-develop

set -e

# コンテナの起動状態を維持するためにフォアグラウンドで動くプロセスが必要なので、とりあえずこれを実行
mc watch ${MINIO_DATA_DIR}
