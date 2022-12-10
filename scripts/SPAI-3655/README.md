```bash
cd scripts/SPAI-3655

# pull aws-cli docker image
docker build . -t spai-3655

# prepare .env file
cp .env.sample .env (and EDIT)

# exec data migration
docker run -it --rm -v $(pwd):/work -v $(pwd)/../../common_module/:/src/common_module -w /work --env-file=.env -e PYTHONPATH=/src spai-3655 python migrate_table.py
```
