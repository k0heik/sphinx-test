import pytest

from common_module.target_unit_util import get_target_unit_df


def success_target_unit_csvs():
    return ["""advertising_account_id,portfolio_id,is_target_unit
1,2,True
1,2,False
2,None,False
3,None,True
""", """advertising_account_id,portfolio_id,is_target_unit
"""]


def failure_target_unit_csvs():
    return ["""advertising_account_id,portfolio_id,is_target_unit1
1,2,True
""", """advertising_account_id,portfolio_id,is_target_unit
1,2,True
1,2,False
2,None,False
3,None,True,1
"""]


@pytest.mark.parametrize("csv", success_target_unit_csvs())
def test_get_target_unit_df(mocker, csv):
    mocker.patch("common_module.target_unit_util.get_ssm_parameter", return_value=csv)

    with pytest.raises(ValueError):
        _ = get_target_unit_df()


@pytest.mark.parametrize("csv", failure_target_unit_csvs())
def test_get_target_unit_df_fail(mocker, csv):
    mocker.patch("common_module.target_unit_util.get_ssm_parameter", return_value=csv)

    with pytest.raises(ValueError):
        _ = get_target_unit_df()
