## コンテナの起動

```bash
docker-compose up -d --build
```

## デプロイ

```
# PrePRD
docker-compose exec cdk cdk deploy -c target_env=PreProduction
# Staging
docker-compose exec cdk cdk deploy -c target_env=Staging
# Production
docker-compose exec cdk cdk deploy -c target_env=Production
# Debug Branch
docker-compose exec cdk cdk deploy -c debug_name=<debug name> -c debug_branch=<debug branch SPAI-xxxx>
```

## aws cdk commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation


## Note
If first run, exec `cdk bootstrap` command.

```
cdk bootstrap --qualifier ${cdk.jsonで定義したqualifier値} -c target_env=${任意の有効なtarget_env}
```
