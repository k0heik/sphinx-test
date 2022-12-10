import os

from common_module.aws_util import get_s3_file_list

from module import args, output_csv, output_json


def s3_output_prefix():
    today = args.Event.today
    advertising_account_id = args.Event.advertising_account_id
    portfolio_id = args.Event.portfolio_id

    y = today.year
    m = today.month
    d = today.day

    stage = os.environ["OUTPUT_STAGE"]

    date_predix = f"{y}/{m:02}/{d:02}"
    if portfolio_id is None:
        file_prefix = f"{y}{m:02}{d:02}_adAccount_{advertising_account_id}_{stage}"
    else:
        file_prefix = (
            f"{y}{m:02}{d:02}_portfolio_{advertising_account_id}_{portfolio_id}_{stage}"
        )

    return date_predix, file_prefix


def is_existing_today_outputs():
    OUTPUT_CSV_BUCKET = os.environ["OUTPUT_CSV_BUCKET"]
    OUTPUT_JSON_BUCKET = os.environ["OUTPUT_JSON_BUCKET"]

    for bucket, path in [
        (OUTPUT_JSON_BUCKET, output_json.s3_output_path()),
        (OUTPUT_CSV_BUCKET, output_csv.s3_output_path(output_csv.CSV_DATA_TYPE_UNIT)),
        (OUTPUT_CSV_BUCKET, output_csv.s3_output_path(output_csv.CSV_DATA_TYPE_CAMPAIGN)),
        (OUTPUT_CSV_BUCKET, output_csv.s3_output_path(output_csv.CSV_DATA_TYPE_AD)),
    ]:
        if len(get_s3_file_list(bucket, path)) != 0:
            return True

    return False
