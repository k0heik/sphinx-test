#!/bin/bash -eu
BEFORE_JSON_DIR=tmpdata/codebuild/`date '+%Y%m%d%H%M%S'`/before
AFTER_JSON_DIR=tmpdata/codebuild/`date '+%Y%m%d%H%M%S'`/after

mkdir -p ${BEFORE_JSON_DIR}
mkdir -p ${AFTER_JSON_DIR}

SYSTEM_NAMES="CAP CTR CVR RPC PID Bid MLValidation MLMetricETL OutputIntegrated Monitoring InsertBiddingInput"
CODEBUILD_NAMES=""
for SYSTEM_NAME in ${SYSTEM_NAMES}
do
    CODEBUILD_NAMES+=" ${TARGET_ENV}OptimiseBiddingML-${SYSTEM_NAME}"
done


function generate_after_json () {
    CODEBUILD_NAME=$1

    ADD_VARS="{
        \"name\": \"DOCKERHUB_USERNAME\",
        \"value\": \"${DOCKERHUB_USERNAME_VALUE}\",
        \"type\": \"PARAMETER_STORE\"
    },{
        \"name\": \"DOCKERHUB_PASSWORD\",
        \"value\": \"${DOCKERHUB_PASSWORD_VALUE}\",
        \"type\": \"PARAMETER_STORE\"
    }
    "

    aws codebuild batch-get-projects --names ${CODEBUILD_NAME} > ${BEFORE_JSON_DIR}/${CODEBUILD_NAME}.json

    cp ${BEFORE_JSON_DIR}/${CODEBUILD_NAME}.json ${AFTER_JSON_DIR}/${CODEBUILD_NAME}.json

    jq ".projects[].environment.environmentVariables |= . + [${ADD_VARS}]" ${AFTER_JSON_DIR}/${CODEBUILD_NAME}.json 1> ${AFTER_JSON_DIR}/${CODEBUILD_NAME}_tmp.json
    cp ${AFTER_JSON_DIR}/${CODEBUILD_NAME}_tmp.json ${AFTER_JSON_DIR}/${CODEBUILD_NAME}.json

    # 残しておくとupdateでエラーになる項目を削除
    jq "del(.projects[].created)" ${AFTER_JSON_DIR}/${CODEBUILD_NAME}.json 1> ${AFTER_JSON_DIR}/${CODEBUILD_NAME}_tmp.json
    cp ${AFTER_JSON_DIR}/${CODEBUILD_NAME}_tmp.json ${AFTER_JSON_DIR}/${CODEBUILD_NAME}.json
    jq "del(.projects[].lastModified)" ${AFTER_JSON_DIR}/${CODEBUILD_NAME}.json 1> ${AFTER_JSON_DIR}/${CODEBUILD_NAME}_tmp.json
    cp ${AFTER_JSON_DIR}/${CODEBUILD_NAME}_tmp.json ${AFTER_JSON_DIR}/${CODEBUILD_NAME}.json
    jq "del(.projects[].badge)" ${AFTER_JSON_DIR}/${CODEBUILD_NAME}.json 1> ${AFTER_JSON_DIR}/${CODEBUILD_NAME}_tmp.json
    cp ${AFTER_JSON_DIR}/${CODEBUILD_NAME}_tmp.json ${AFTER_JSON_DIR}/${CODEBUILD_NAME}.json
    jq "del(.projects[].arn)" ${AFTER_JSON_DIR}/${CODEBUILD_NAME}.json 1> ${AFTER_JSON_DIR}/${CODEBUILD_NAME}_tmp.json
    cp ${AFTER_JSON_DIR}/${CODEBUILD_NAME}_tmp.json ${AFTER_JSON_DIR}/${CODEBUILD_NAME}.json

    rm ${AFTER_JSON_DIR}/${CODEBUILD_NAME}_tmp.json

    if [ -z "`cat ${AFTER_JSON_DIR}/${CODEBUILD_NAME}.json`" ]; then
        echo "[ERROR] empty file: ${CODEBUILD_NAME}"
        exit 1
    fi
}


for CODEBUILD_NAME in ${CODEBUILD_NAMES}
do
    echo "get and generate json: ${CODEBUILD_NAME}"
    generate_after_json ${CODEBUILD_NAME}
done

read -p "exec update? if you want to exec, type 'yes': " RES
if [ "$RES" != "yes" ]; then
    echo "skip update."
else
    for CODEBUILD_NAME in ${CODEBUILD_NAMES}
    do
        TMP_JSON_PATH=/tmp/${CODEBUILD_NAME}.json
        jq '.projects[0]' ${AFTER_JSON_DIR}/${CODEBUILD_NAME}.json > ${TMP_JSON_PATH}
        echo "add env: ${CODEBUILD_NAME}"
        aws codebuild update-project --cli-input-json file://${TMP_JSON_PATH}
        echo "delete vpc: ${CODEBUILD_NAME}"
        aws codebuild update-project --name ${CODEBUILD_NAME} --vpc-config={}
    done
fi
