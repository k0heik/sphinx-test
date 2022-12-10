import datetime
import pytest

from module import libs


@pytest.mark.parametrize("is_portfolio_id_none", [True, False])
def test_s3_output_prefix(mocker, monkeypatch, is_portfolio_id_none):
    mocker.patch("module.args.Event.today", datetime.datetime(2021, 1, 1))
    mocker.patch("module.args.Event.advertising_account_id", 1)
    monkeypatch.setenv("OUTPUT_STAGE", "output_stage")

    if is_portfolio_id_none:
        mocker.patch("module.args.Event.portfolio_id", None)
    else:
        mocker.patch("module.args.Event.portfolio_id", 3)

    date_prefix, file_prefix = libs.s3_output_prefix()

    assert date_prefix == "2021/01/01"
    if is_portfolio_id_none:
        assert file_prefix == "20210101_adAccount_1_output_stage"
    else:
        assert file_prefix == "20210101_portfolio_1_3_output_stage"
