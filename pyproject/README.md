## how to update python library dependencies

### prepare contariner

```
cd pyproject
docker pull python:3.8
docker run -it --rm -v $(pwd):/app -w /app -e PYTHONDONTWRITEBYTECODE=1  -e PYTHONPATH=/app docker.io/library/python:3.8 bash
```

### in contariner

1. install poetry
```
pip install poetry==1.2.0
```

2. update lock file
```
poetry lock --no-update
```
