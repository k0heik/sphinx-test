import io
import os
import boto3
import pandas as pd
from io import StringIO, BytesIO
from common_module.system_util import get_bucket_key_from_s3_uri


def _minio_params():
    MINIO_URL = os.environ.get('MINIO_URL', None)
    if MINIO_URL is None or MINIO_URL == "":
        return {}

    return {
        "endpoint_url": MINIO_URL,
        "aws_access_key_id": os.environ['MINIO_ROOT_USER'],
        "aws_secret_access_key": os.environ['MINIO_ROOT_PASSWORD'],
    }


def s3_client():
    return boto3.client('s3', **_minio_params())


def s3_resource():
    return boto3.resource('s3', **_minio_params())


def get_ssm_parameter(parameter_name: str) -> str:
    """指定したパラメータ名の値をSSMから取得して返す関数

    Args:
        parameter_name (str): SSMのパラメータ名
    """
    ssm_client = boto3.client('ssm')
    response = ssm_client.get_parameters(
        Names=[
            parameter_name
        ],
        WithDecryption=True
    )
    return response['Parameters'][0]['Value']


def write_df_to_s3(df, bucket, key, index=False, header=True):
    """指定のデータフレームをCSV形式でS3に書き出す

    Args:
        df (pands.DataFrame): 出力するデータフレーム
        bucket (str): 出力先のバケット名
        key (str): 出力するパス
        index (boolean): インデックスを出力するか
        header (boolean): ヘッダーを出力するか
    """

    file_buffer = StringIO()
    file_buffer.write(df.to_csv(index=index, header=header))

    s3_client().upload_fileobj(
        BytesIO(file_buffer.getvalue().encode()),
        bucket,
        key,
    )


def write_json_to_s3(json: str, bucket, key):
    json_buffer = StringIO()
    json_buffer.write(json)
    bytes_buffer = BytesIO(json_buffer.getvalue().encode())
    s3_client().upload_fileobj(bytes_buffer, bucket, key)


def write_binary_to_s3(bin, bucket, key):
    s3_client().put_object(Bucket=bucket, Key=key, Body=bin)


def get_s3_file_list(bucket, prefix):
    res = s3_resource().Bucket(bucket).objects.filter(Prefix=prefix)

    return [k.key for k in res]


def copy_s3_file(
    source_bucket, source_key, destination_bucket, destination_key
):
    copy_source = {
        'Bucket': source_bucket,
        'Key': source_key
    }

    s3_resource().meta.client.copy(
        copy_source, destination_bucket, destination_key)


def read_binary_from_s3(bucket, key):
    """指定のデータを取得する
    Args:
        bucket (str): 取得先のバケット名
        key (str): 取得するパス
    """
    response = s3_client().get_object(Bucket=bucket, Key=key)
    return response['Body'].read()


def read_from_s3(
    bucket,
    key,
    encoding='utf-8',
    dtype=None,
    parse_dates=False
) -> pd.DataFrame:
    """指定のデータ（CSV形式）を取得して文字列で返す
    Args:
        bucket (str): 取得先のバケット名
        key (str): 取得するパス
    """
    body = read_binary_from_s3(bucket, key).decode(encoding)

    return pd.read_csv(io.StringIO(body), dtype=dtype, parse_dates=parse_dates)


def read_from_s3_uri(
    s3_uri,
    encoding='utf-8',
    dtype=None,
    parse_dates=False
) -> pd.DataFrame:
    bucket, key = get_bucket_key_from_s3_uri(s3_uri)

    return read_from_s3(
        bucket, key, encoding, dtype, parse_dates)


def load_file_list(bucket, prefix):
    res = s3_client().list_objects_v2(
        Bucket=bucket,
        Prefix=prefix if prefix[-1] == "/" else f"{prefix}/",
        Delimiter='/',
    )

    if int(res['KeyCount']) == 0:
        return []

    return [content['Key'] for content in res['Contents']]
