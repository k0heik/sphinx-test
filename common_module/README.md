# How to run only common_module UT
```bash
cd common_module

cp tests/.env.sample tests/.env (and EDIT)

docker build -t bid_optimisation_ml_common_module -f ./Dockerfile ..

# exec pytest
docker run -it --rm -v $(pwd):/app/common_module -w /app -e PYTHONDONTWRITEBYTECODE=1 -e PYTHONPATH=/app --env-file=tests/.env bid_optimisation_ml_common_module pytest -o cache_dir=/tmp

# exec flake8
docker run -it --rm -v $(pwd):/app/common_module -w /app/common_module -e PYTHONDONTWRITEBYTECODE=1 -e PYTHONPATH=/app --env-file=tests/.env bid_optimisation_ml_common_module flake8
```
