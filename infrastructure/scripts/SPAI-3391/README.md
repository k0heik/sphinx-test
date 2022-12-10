```bash
cd infrastructure/scripts/SPAI-3391

# pull aws-cli docker image
docker build . -t spai-3391

# prepare .env file
cp .env.sample .env (and EDIT)

# execute change codepieline role command
docker run -it --rm -v $(pwd):/work -w /work --env-file=.env --entrypoint "" spai-3391 bash /work/change_codepipeline_role.sh

# execute change codebuild role command
docker run -it --rm -v $(pwd):/work -w /work --env-file=.env --entrypoint "" spai-3391 bash /work/change_codebuild_role.sh

# execute change codebuild settings command
docker run -it --rm -v $(pwd):/work -w /work --env-file=.env --entrypoint "" spai-3391 bash /work/change_codebuild_settings.sh
```
