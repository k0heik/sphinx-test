#!/bin/bash -eu

BEFORE_JSON_DIR=tmpdata/codepipeline/`date '+%Y%m%d%H%M%S'`/before
AFTER_JSON_DIR=tmpdata/codepipeline/`date '+%Y%m%d%H%M%S'`/after

mkdir -p ${BEFORE_JSON_DIR}
mkdir -p ${AFTER_JSON_DIR}

CODEPIPELINE_NAME=${TARGET_ENV}OptimiseBiddingML

aws codepipeline get-pipeline --name ${CODEPIPELINE_NAME} > ${BEFORE_JSON_DIR}/${CODEPIPELINE_NAME}.json


echo "s/${BEFORE_CODEPIPELINE_ROLE_ARN//\//\\/}/${AFTER_CODEPIPELINE_ROLE_ARN//\//\\/}/g"

sed -e "s/${BEFORE_CODEPIPELINE_ROLE_ARN//\//\\/}/${AFTER_CODEPIPELINE_ROLE_ARN//\//\\/}/g" ${BEFORE_JSON_DIR}/${CODEPIPELINE_NAME}.json > ${AFTER_JSON_DIR}/${CODEPIPELINE_NAME}.json


if [ -z "`cat ${AFTER_JSON_DIR}/${CODEPIPELINE_NAME}.json`" ]; then
    echo "[ERROR] empty file: ${CODEPIPELINE_NAME}"
    exit 1
fi

DIFF=`diff ${BEFORE_JSON_DIR}/${CODEPIPELINE_NAME}.json ${AFTER_JSON_DIR}/${CODEPIPELINE_NAME}.json`

echo $DIFF

if [ -z "$DIFF" ]; then
    echo "[ERROR] no change: ${CODEPIPELINE_NAME}"
    exit 1
fi

CODEPIPELINE_JSON=`cat ${AFTER_JSON_DIR}/${CODEPIPELINE_NAME}.json | jq '.pipeline'`

echo ${CODEPIPELINE_JSON}

read -p "exec update? if you want to exec, type 'yes': " RES
if [ "$RES" != "yes" ]; then
    echo "skip update."
else
    aws codepipeline update-pipeline --pipeline "${CODEPIPELINE_JSON}"
fi