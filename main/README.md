## localでのテスト方法

### 最新のコードで動作確認
```bash
docker-compose up -d --build

# call from container main
docker-compose exec lambda python test_local.py ${date (YYYY-MM-DD) or "latest"} -a ${advertising_account_id} [-p ${portfolio_id}]

# call from container get_units
docker-compose exec lambda python test_local_get_units.py ${date (YYYY-MM-DD)}

# exec pytest
docker-compose exec lambda python -m pytest

# exec flake8
docker-compose exec lambda python -m flake8
```

### Login Container
```bash
docker-compose up -d
docker-compose exec lambda bash
```

### How to Check MinIO data with Web Console
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
