import datetime
import pandas as pd

from module import cvr_prediction


def test_exec(mocker):
    mocker.patch("module.args.Event.today", datetime.datetime.today)
    mock = mocker.patch("module.cvr_prediction.CVRPredictionService.predict")
    df = pd.DataFrame()
    cvr_prediction.exec(df, df)

    mock.assert_called_once
