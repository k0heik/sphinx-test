```bash
cd infrastructure/cloudformation

# if use with Docker
docker pull amazon/aws-cli

cp .env.sample .env (and EDIT)

# deploy stack
docker run -it --rm -v $(pwd):/work -w /work --env-file=.env --entrypoint="" amazon/aws-cli bash deploy.sh ${target_env} ${target_config_name} [-y] [-d ${test_param_name}] [-p name1=val1 ...]
e.g.
docker run -it --rm -v $(pwd):/work -w /work --env-file=.env --entrypoint="" amazon/aws-cli bash deploy.sh PreProduction CodePipeline
docker run -it --rm -v $(pwd):/work -w /work --env-file=.env --entrypoint="" amazon/aws-cli bash deploy.sh PreProduction CodePipeline -d SPAI-XXXX -p DebugBranch=feature/SPAI-XXXX -p ex_param=ex_var

or

docker run -it --rm -v $(pwd):/work -w /work --env-file=.env --entrypoint="" amazon/aws-cli bash deploy.sh ${target_env} ${target_dir_name} [-y] [-d ${test_param_name}] [-p name1=val1 ...]
e.g.
docker run -it --rm -v $(pwd):/work -w /work --env-file=.env --entrypoint="" amazon/aws-cli bash deploy.sh PreProduction auto-deploy
docker run -it --rm -v $(pwd):/work -w /work --env-file=.env --entrypoint="" amazon/aws-cli bash deploy.sh PreProduction auto-deploy -d SPAI-XXXX -p ex_param1=ex_var1 -p ex_param2=ex_var2
```
