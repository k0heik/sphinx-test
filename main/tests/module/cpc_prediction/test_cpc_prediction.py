import datetime
import pandas as pd

from module import cpc_prediction


def test_exec(mocker):
    mocker.patch("module.args.Event.today", datetime.datetime.today)
    mock = mocker.patch("module.cpc_prediction.CPCPredictionService.predict")
    df = pd.DataFrame()
    cpc_prediction.exec(df, df)

    mock.assert_called_once
