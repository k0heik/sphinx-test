# ci.yml生成テンプレート及びスクリプト

以下で.github/workflows/ci.ymlを生成する
```bash
pip install jinja2
python render_ci.py
```

※生成対象のサブシステムは，render_ci.pyに記載

# How to run on Docker
```bash
docker pull python:3.8

# login on root
docker run -it --rm -v $(pwd):/app -w /app -e PYTHONDONTWRITEBYTECODE=1 docker.io/library/python:3.8 bash

# on Container
pip install jinja2
cd deploy
python render_ci.py
```
