import datetime
import pandas as pd

from module import spa_prediction


def test_exec(mocker):
    mocker.patch("module.args.Event.today", datetime.datetime.today)
    mock = mocker.patch("module.spa_prediction.SPAPredictionService.predict")
    df = pd.DataFrame()
    spa_prediction.exec(df, df)

    mock.assert_called_once
