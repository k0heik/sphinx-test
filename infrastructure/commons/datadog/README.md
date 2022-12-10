## DataDog monitor Create or Update

Files in this dir are called by buildspec.yml


### How to Test
```
cp infrastructure/commons/datadog/.env.sample infrastructure/commons/datadog/.env
(and EDIT)

docker pull python:3.8

# login on root
docker run -it --rm -v $(pwd):/app -w /app -e PYTHONDONTWRITEBYTECODE=1 --env-file=infrastructure/commons/datadog/.env docker.io/library/python:3.8 bash

# on Container
pip install -r /app/infrastructure/commons/datadog/requirements.txt
cd /app/infrastructure/${subsystem dir including datadog_config.yml and serverless.yml}
python ../commons/datadog/deploy_monitor.py ${target_env}

e.g.
pip install -r /app/infrastructure/commons/datadog/requirements.txt
pip install Jinja2==2.11.3 pyyaml==5.4.1
cd /app/infrastructure/OptimiseBiddingML-OutputIntegrated && python ../commons/render_serverless.py && python ../commons/datadog/deploy_monitor.py Staging

(Note: Not commit auto generated serverless.yml)
```
