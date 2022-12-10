# How to run only sophia-ai UT
```bash
cd sophia-ai

docker build -t bid_optimisation_ml_sophia_ai -f ./Dockerfile ..

# exec pytest
docker run -it --rm -v $(pwd):/app -w /app -e PYTHONDONTWRITEBYTECODE=1 -e PYTHONPATH=/app bid_optimisation_ml_sophia_ai pytest -o cache_dir=/tmp

# exec flake8
docker run -it --rm -v $(pwd):/app -w /app -e PYTHONDONTWRITEBYTECODE=1 -e PYTHONPATH=/app bid_optimisation_ml_sophia_ai flake8
```
