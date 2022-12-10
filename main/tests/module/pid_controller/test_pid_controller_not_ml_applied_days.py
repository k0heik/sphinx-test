import pytest

import pandas as pd

from spai.service.pid.config import ML_LOOKUP_DAYS

from module import pid_controller


@pytest.mark.parametrize(
    "is_lastday_ml_applied, is_ml_applied_history_none",
    [
        (True, True),
        (True, False),
        (False, False),
    ],
)
@pytest.mark.parametrize("is_ml_applied_history_zero", [True, False])
def test_pid_controller_not_ml_applied_days(
    common_data_yesterday,
    ml_applied_history_df,
    is_lastday_ml_applied,
    is_ml_applied_history_none,
    is_ml_applied_history_zero,
):
    yesterday = common_data_yesterday

    if is_ml_applied_history_none:
        ml_applied_history_df = None

    if is_ml_applied_history_zero:
        ml_applied_history_df = pd.DataFrame()

    result = pid_controller._not_ml_applied_days(
        yesterday, is_lastday_ml_applied, ml_applied_history_df
    )

    if is_lastday_ml_applied:
        assert result == 0
    elif is_ml_applied_history_zero:
        assert result == ML_LOOKUP_DAYS
    else:
        assert result == 1
