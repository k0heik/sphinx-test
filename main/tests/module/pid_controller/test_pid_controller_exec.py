import pandas as pd

import pytest

from module import pid_controller


def test_pid_controller_exec(
    mocker, common_data_today, pid_controller_input_df, campaign_all_actual_df,
):
    mocker.patch("module.args.Event.today", common_data_today)
    mocker.patch(
        "module.pid_controller._prepare_df", return_value=pid_controller_input_df)

    df = pid_controller.exec(None, None, None, None, None, campaign_all_actual_df, None, None, None)

    assert isinstance(df, pd.DataFrame)


@pytest.mark.parametrize("exception_class", [Exception, ValueError])
def test_pid_controller_exec_exception(
    mocker, common_data_today, pid_controller_input_df, campaign_all_actual_df, exception_class
):
    mocker.patch("module.args.Event.today", common_data_today)
    mocker.patch(
        "module.pid_controller._prepare_df", return_value=pid_controller_input_df)
    mocker.patch(
        "spai.optim.pid._init_pq", side_effect=exception_class)

    df = pid_controller.exec(None, None, None, None, None, campaign_all_actual_df, None, None, None)

    assert df is None
