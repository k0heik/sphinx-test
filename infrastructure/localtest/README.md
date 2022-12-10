### How to Test on PC

```
SOURCE_ROOT=${/abspath/to/srcdir}
cd ${SOURCE_ROOT}
e.g.
cd ../ && SOURCE_ROOT=`pwd`

git clone git@github.com:aws/aws-codebuild-docker-images.git
cd aws-codebuild-docker-images/ubuntu/standard/5.0
docker build -t aws/codebuild/standard:5.0 .

cp ${SOURCE_ROOT}/bid_optimisation_ml/infrastructure/localtest/.env.sample ${SOURCE_ROOT}/bid_optimisation_ml/infrastructure/localtest/.env
(and EDIT)

${abspath/to/aws-codebuild-docker-images}/local_builds/codebuild_build.sh \
    -i aws/codebuild/standard:5.0 \
    -a /tmp \
    -s ${abspath/to/thisrepositorydir} \
	-b ${abspath/to/buildspec.yml} \
	-e ${abspath/to/buildenvfile}

e.g.
${SOURCE_ROOT}/aws-codebuild-docker-images/local_builds/codebuild_build.sh \
    -i aws/codebuild/standard:5.0 \
    -a /tmp \
    -s ${SOURCE_ROOT}/bid_optimisation_ml/ \
	-b ${SOURCE_ROOT}/bid_optimisation_ml/infrastructure/OptimiseBiddingML-OutputIntegrated/buildspec.yml \
	-e ${SOURCE_ROOT}/bid_optimisation_ml/infrastructure/localtest/.env
```

#### Notes

If build test on local, buildspec.yml needs change about follows.

 - Set `runtime-versions` to `python: 3.8` or `python: 3.9` instead of `python: latest`.
 - Define SSM parameters same with environs of CodeBuild to `env.parameter-store` .
