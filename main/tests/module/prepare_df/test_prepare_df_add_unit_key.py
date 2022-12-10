import pandas as pd
import pytest

from module import prepare_df


@pytest.fixture
def df():
    n = 10
    return pd.DataFrame(
        {
            "campaign_id": range(1, n + 1),
            "params": ["param"] * 10,
        }
    )


@pytest.mark.parametrize("portfolio_id", [1, None])
def test_add_unit_key(mocker, df, portfolio_id):
    advertising_account_id = 9999
    mocker.patch("module.args.Event.advertising_account_id", advertising_account_id)
    mocker.patch("module.args.Event.portfolio_id", portfolio_id)

    result_df = prepare_df.add_unit_key(df)

    assert len(result_df) == len(df)
    assert list(result_df["advertising_account_id"].values) == [
        advertising_account_id
    ] * len(result_df)
    if portfolio_id is None:
        assert result_df["portfolio_id"].isnull().all()
    else:
        assert list(result_df["portfolio_id"].values) == [portfolio_id] * len(result_df)
    assert result_df["portfolio_id"].dtype == "Int64"
