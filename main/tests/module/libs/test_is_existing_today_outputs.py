import uuid
import datetime
from unittest import mock

import pytest

from module import libs


@pytest.mark.parametrize("s3_output_prefix_value, expected", [
    ([], False),
    ([None], True),
    (["path"], True),
])
def test_is_existing_today_outputs(mocker, s3_output_prefix_value, expected):
    mocker.patch("module.args.Event.today", datetime.datetime(2021, 1, 1))
    mocker.patch("module.args.Event.advertising_account_id", 1)
    mocker.patch("module.args.Event.portfolio_id", 3)

    mocker.patch("module.libs.get_s3_file_list", return_value=s3_output_prefix_value)

    assert libs.is_existing_today_outputs() is expected


def test_is_existing_today_outputs_args_check(mocker, monkeypatch):
    mocker.patch("module.args.Event.today", datetime.datetime(2021, 1, 1))
    mocker.patch("module.args.Event.advertising_account_id", 1)
    mocker.patch("module.args.Event.portfolio_id", 3)

    OUTPUT_JSON_BUCKET = f"{uuid.uuid4()}"
    OUTPUT_JSON_PREFIX = f"{uuid.uuid4()}"
    OUTPUT_CSV_BUCKET = f"{uuid.uuid4()}"
    OUTPUT_CSV_PREFIX = f"{uuid.uuid4()}"
    OUTPUT_STAGE = f"{uuid.uuid4()}"

    monkeypatch.setenv("OUTPUT_JSON_BUCKET", OUTPUT_JSON_BUCKET)
    monkeypatch.setenv("OUTPUT_JSON_PREFIX", OUTPUT_JSON_PREFIX)
    monkeypatch.setenv("OUTPUT_CSV_BUCKET", OUTPUT_CSV_BUCKET)
    monkeypatch.setenv("OUTPUT_CSV_PREFIX", OUTPUT_CSV_PREFIX)
    monkeypatch.setenv("OUTPUT_STAGE", OUTPUT_STAGE)

    m = mock.Mock(return_value=[])
    mocker.patch("module.libs.get_s3_file_list", m)

    assert not libs.is_existing_today_outputs()

    m.assert_has_calls([
        mock.call(
            OUTPUT_JSON_BUCKET, f"{OUTPUT_JSON_PREFIX}/2021/01/01/20210101_portfolio_1_3_{OUTPUT_STAGE}.json"),
        mock.call(
            OUTPUT_CSV_BUCKET, f"{OUTPUT_CSV_PREFIX}/2021/01/01/unit/20210101_portfolio_1_3_{OUTPUT_STAGE}_unit.csv"),
        mock.call(
            OUTPUT_CSV_BUCKET,
            f"{OUTPUT_CSV_PREFIX}/2021/01/01/campaign/20210101_portfolio_1_3_{OUTPUT_STAGE}_campaign.csv"),
        mock.call(
            OUTPUT_CSV_BUCKET, f"{OUTPUT_CSV_PREFIX}/2021/01/01/ad/20210101_portfolio_1_3_{OUTPUT_STAGE}_ad.csv"),
    ])
