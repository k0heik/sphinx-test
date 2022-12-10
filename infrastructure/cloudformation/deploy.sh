#!/bin/bash
SERVICE_NAME=OptimiseBiddingML

PROVICED_CONFIG_CATEGORIES=`find definitions -name "*.yml" | awk -F'[/.]' '{print $2}' | sed "s/ //g" | uniq`
PROVICED_CONFIG_NAMES=`find definitions -name "*.yml" | awk -F'[/.]' '{print $3}' | sed "s/ //g"`
if [ $# -lt 2 ]; then
  echo "
Usage:
  deploy.sh target_env target_config_name [test_param_name] [-y] [-d DEBUG_NAME] [-p name1=var1 -p name2=var2...]

Options:
    -y                    answer yes for all questions
    -d                    DEBUG_NAME
    -p                    any variable. name=var
"
  exit
fi

YES_FOR_ALL=0
TARGET_ENV=$1
TARGET_CONFIG=$2
DEBUG_NAME=
ADDITIONAL_PARAMS=()
shift 2
while [ "$#" -ge "1" ]; do
  case $1 in
    -y)
      YES_FOR_ALL=1
      ;;
    -d)
      shift
      DEBUG_NAME=$1
      ;;
    -p)
      shift
      ADDITIONAL_PARAMS=(${ADDITIONAL_PARAMS[@]} $1)
      ;;
  esac
  shift
done

CATEGORY_CHECK=`echo ${PROVICED_CONFIG_CATEGORIES} | grep -w ${TARGET_CONFIG}`
NAME_CHECK=`echo ${PROVICED_CONFIG_NAMES} | grep -w ${TARGET_CONFIG}`
EXEC_TYPE=""

if [ -n "$CATEGORY_CHECK" ]; then
  EXEC_TYPE=category
elif [ -n "$NAME_CHECK" ]; then
  EXEC_TYPE=name
fi

if [ -z "$EXEC_TYPE" ]; then
  echo "[${TARGET_CONFIG}] is invalid. Please type correctly config name or category."
  echo "target_config_name:"
  for N in ${PROVICED_CONFIG_NAMES}; do echo " ${N}"; done
  echo "target_config_categories:"
  for N in ${PROVICED_CONFIG_CATEGORIES}; do echo " ${N}"; done
  exit
fi

if [ $EXEC_TYPE == "category" ]; then
  DEFINITIONS_FILE_PATHS=`find definitions/${TARGET_CONFIG} -name "*.yml"`
elif [ $EXEC_TYPE == "name" ]; then
  DEFINITIONS_FILE_PATHS=`find definitions -name "${TARGET_CONFIG}.yml"`
fi
echo "deploy definition paths:"
echo "${DEFINITIONS_FILE_PATHS}"

for DEFINITIONS_FILE_PATH in ${DEFINITIONS_FILE_PATHS}; do
  aws cloudformation validate-template \
    --template-body "`cat ${DEFINITIONS_FILE_PATH}`"

  if [ $? -ne 0 ]; then exit 1; fi

  cat ${DEFINITIONS_FILE_PATH}
  echo ""

  TARGET_CONFIG_NAME=${DEFINITIONS_FILE_PATH##*/}
  TARGET_CONFIG_NAME=${TARGET_CONFIG_NAME%.*}
  CLOUDFORMATION_STACK_NAME=${DEBUG_NAME}${TARGET_ENV}${SERVICE_NAME}-${TARGET_CONFIG_NAME}
  COMMAND="aws cloudformation deploy \
    --stack-name ${CLOUDFORMATION_STACK_NAME} \
    --template-file ${DEFINITIONS_FILE_PATH} \
    --parameter-overrides EnvType=${TARGET_ENV} DebugName=${DEBUG_NAME} ${ADDITIONAL_PARAMS[@]}\
    --capabilities CAPABILITY_NAMED_IAM"

  echo "CLOUDFORMATION_STACK_NAME=${CLOUDFORMATION_STACK_NAME}"
  echo "TARGET_ENV=${TARGET_ENV}"
  echo "DEBUG_NAME=${DEBUG_NAME}"
  echo "ADDITIONAL_PARAMS: ${ADDITIONAL_PARAMS[@]}"
  echo "COMMAND: ${COMMAND}"

  if [[ ${YES_FOR_ALL} -eq 0 ]]; then
    echo 'Deploy? [Y/n]: '
    read REP
    if [[ $REP != "Y" ]]; then continue; fi
  fi

  ${COMMAND}

  if [ $? -ne 0 ]; then exit 1; fi
done

echo "cloudformation deploy finished."
