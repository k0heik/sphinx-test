#!/bin/bash -eu
SYSTEM_NAMES="CAP CTR CVR RPC PID Bid MLValidation MLMetricETL OutputIntegrated Monitoring InsertBiddingInput Pipeline TrainPipeline"
CODEBUILD_NAMES="${TARGET_ENV}OptimiseBiddingMLCommon-SlackNotification"
for SYSTEM_NAME in ${SYSTEM_NAMES}
do
    CODEBUILD_NAMES+=" ${TARGET_ENV}OptimiseBiddingML-${SYSTEM_NAME}"
done

read -p "exec update? if you want to exec, type 'yes': " RES
if [ "$RES" != "yes" ]; then
    echo "skip update."
else
    for CODEBUILD_NAME in ${CODEBUILD_NAMES}
    do
        echo ${CODEBUILD_NAME}
        aws codebuild update-project --name ${CODEBUILD_NAME} --service-role ${AFTER_CODEBUILD_ROLE_ARN}
    done
fi
