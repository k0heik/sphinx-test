# ローカルでの実行方法

## exec docker-compose

```bash
docker-compose build
docker-compose up -d

# login container
docker-compose exec batch sh
```

## .envファイルの作成

`.env.sample`の内容を参考に各項目を設定する

## exec batch features
```bash
docker-compose up -d

# exec train
docker-compose exec batch python main.py train ${date (YYYY-MM-DD)}

# exec test
docker-compose exec batch python -m pytest

# check code style
docker-compose exec batch python -m flake8
```

### How to Check MinIO data with Web Conole
1. Open http://localhost:9091/
2. Login with Username & Password witten in docker-compose.yml

### How to Check MinIO data with CUI (Bucket Access Policy needs to be changed to "public")

#### view test file

e.g.
```
curl http://localhost:9090/optimise-bidding-ml-staging/main/csv/2022/11/07/ad/20221107_portfolio_1478_122_staging_ad.csv
```

#### download binary file

e.g.
```
curl -o latest_model.bin http://localhost:9090/optimise-bidding-ml-staging/cpc/model/latest.bin
```
