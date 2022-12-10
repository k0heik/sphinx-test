import re
import datetime
from dateutil import parser


def get_target_date(date):
    if date == "latest":
        date = datetime.datetime.today()
    else:
        date = parser.parse(date)

    return date.replace(hour=0, minute=0, second=0, microsecond=0)


def get_s3_key(
    prefix: str,
    today: datetime.datetime,
    suffix: str,
) -> str:
    key = (
        f"{prefix}"
        f"{today.year}/{today.month:02}/{today.day:02}/"
        f"{suffix}"
    )
    return key


def get_bucket_key_from_s3_uri(s3_uri: str):
    m = re.match("^s3://(.+?)/(.+)$", s3_uri)

    if m is None:
        raise ValueError("s3_uri format is invalid")

    return m.groups()[0], m.groups()[1]
