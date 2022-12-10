```bash
cd scripts/SPAI-4172

# pull aws-cli docker image
docker build . -t spai-4172

# prepare .env file
cp .env.sample .env (and EDIT)

# exec data migration
docker run -it --rm -v $(pwd):/work -v $(pwd)/../../common_module/:/src/common_module -w /work --env-file=.env -e PYTHONPATH=/src spai-4172 python convert_portfolio_id.py
```
