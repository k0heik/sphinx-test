import argparse
import json
import logging
import os
import time
import typing
import datetime

import boto3

logger = logging.getLogger(__name__)
fmt = "%(asctime)s %(levelname)s %(name)s :%(message)s"
logging.basicConfig(level=logging.INFO, format=fmt)
logger.setLevel(logging.INFO)


os.environ["AWS_DEFAULT_REGION"] = "ap-northeast-1"

BASE_ARN = "arn:aws:states:ap-northeast-1:781667190002:stateMachine"
STAGE2ARN = {
    "develop": None,
    "staging": (f"{BASE_ARN}:StagingOptimiseBiddingML-PID"),
    "preproduction": f"{BASE_ARN}:PreProductionOptimiseBiddingML-PID",
    "production": f"{BASE_ARN}:ProductionOptimiseBiddingML-PID",
}


def exec_step_function(sm_arn: str, data: str) -> str:
    if not isinstance(sm_arn, str):
        raise ValueError(f"ARN must be str but {sm_arn}")
    stepFunction = boto3.client("stepfunctions")
    response = stepFunction.start_execution(
        stateMachineArn=sm_arn,
        input=data,
    )
    logger.info(f"Started {sm_arn}")
    logger.info(f"response: {response}")
    logger.info(f"data: {data}")
    return response.get("executionArn")


def get_state(execution_arn: str) -> str:
    stepFunction = boto3.client("stepfunctions")
    response = stepFunction.describe_execution(executionArn=execution_arn)
    return response.get("status", "NOT_FOUND")


def wait_until_done(execution_arn: str, check_interval: int = 15) -> bool:
    assert check_interval >= 5  # care about the api rate limit
    state = ""
    while True:
        state = get_state(execution_arn)
        logger.info(f"State of {execution_arn} is {state} now.")
        if state in {"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"}:
            break
        time.sleep(check_interval)
    return state == "SUCCEEDED"


def exec_and_wait(sm_arn: str, data: str, check_interval: int = 15) -> bool:
    execution_arn = exec_step_function(sm_arn, data)
    return wait_until_done(execution_arn, check_interval)


def get_parameter_data(year: int, month: int, day: int) -> str:
    data = {
        "batch": {"parameters": {"date": f"{year:0>4}-{month:0>2}-{day:0>2}"}}
    }
    return json.dumps(data)


def seq_exec_sync(
    sm_arn: str,
    data_list: typing.List[str],
    dryrun: bool,
    num_retries: int = 3,
    check_interval: int = 15,
    exec_interval: int = 30,
) -> None:
    for data in data_list:
        for i in range(num_retries):
            if dryrun:
                logger.info(f"Dry run: data = {data}")
                break
            time.sleep(exec_interval)
            ok = exec_and_wait(sm_arn, data, check_interval)
            if ok:
                logger.info(f"Succeeded {sm_arn}")
                break
            else:
                logger.warning(f"Retrying {sm_arn} at {i + 1} / {num_retries}")
        else:
            raise ValueError(f"Could not succeed {sm_arn} for {data}.")


def date_type(date_str):
    return datetime.date.fromisoformat(date_str)


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Execute a Step Functionmonth-to-date (from 1st to yesterday)"
        )
    )
    parser.add_argument(
        "--stage",
        help="stage[staging, preproduction, production]",
        choices=["staging", "preproduction", "production"],
    )
    parser.add_argument("--date", type=date_type, help="date of today")
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument(
        "--num_retries",
        type=int,
        help="num_retries for step functions",
        default=1,
    )
    parser.add_argument(
        "--check_interval",
        type=int,
        help="polling interval [sec]",
        default=15,
    )
    parser.add_argument(
        "--exec_interval",
        type=int,
        help="execution interval [sec]",
        default=30,
    )
    args = parser.parse_args()
    parameters = [
        get_parameter_data(args.date.year, args.date.month, day)
        for day in range(1, args.date.day)
    ]
    logger.info(f"#Exec Step Functions = {len(parameters)}")
    seq_exec_sync(
        STAGE2ARN.get(args.stage),
        parameters,
        args.dry_run,
        args.num_retries,
        args.check_interval,
        args.exec_interval,
    )
    logger.info("Done")


if __name__ == "__main__":
    main()
