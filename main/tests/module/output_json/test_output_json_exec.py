from module import output_json


def test_output_json_exec(mocker, output_json_today, output_json_input_df):
    mocker.patch("module.args.Event.today", output_json_today)
    mocker.patch(
        "module.args.Event.advertising_account_id",
        output_json_input_df["advertising_account_id"].values[0],
    )
    mocker.patch(
        "module.args.Event.portfolio_id", output_json_input_df["portfolio_id"].values[0]
    )
    mocker.patch(
        "module.output_json.output_json._prepare_df", return_value=output_json_input_df
    )
    output_json.exec(None, None, None, None)
