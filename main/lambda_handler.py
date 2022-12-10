import os
import traceback
import pandas as pd

from common_module.logger_util import get_custom_logger
from common_module.system_util import get_target_date

from module import (
    extract,
    prepare_df,
    cpc_prediction,
    cvr_prediction,
    spa_prediction,
    target_pause,
    ml_apply,
    pid_controller,
    bid_optimiser,
    cap_daily_budget,
    output_json,
    output_csv,
)
from module.args import Event
from module.libs import is_existing_today_outputs

logger = get_custom_logger()


def _generate_it_mock_kpi_predictions(it_mock_kpi_predictions):
    def _generate(name):
        df = pd.DataFrame({
            "ad_type": [ad["ad_type"] for ad in it_mock_kpi_predictions],
            "ad_id": [ad["ad_id"] for ad in it_mock_kpi_predictions],
            name: [ad[name] for ad in it_mock_kpi_predictions],
        })
        df["date"] = Event.today

        return df

    return _generate("cpc"), _generate("cvr"), _generate("spa"),


def main(date, advertising_account_id, portfolio_id, it_mock_kpi_predictions=None):
    assert date is not None
    assert advertising_account_id is not None

    Event.advertising_account_id = int(advertising_account_id)
    Event.portfolio_id = int(portfolio_id) if portfolio_id is not None else None
    Event.today = get_target_date(date)

    if is_existing_today_outputs():
        msg = (
            "Output files does already exist. "
            f"advertising_account_id={Event.advertising_account_id},"
            f"portfolio_id={Event.portfolio_id},"
            f"date={Event.today}"
        )
        if os.environ.get("RERUNNABLE", "no") != "yes":
            raise Exception(msg)
        else:
            logger.info(msg)

    logger.info("extract")
    (
        unit_info_df,
        campaign_info_df,
        campaign_all_actual_df,
        ad_info_df,
        ad_target_actual_df,
        daily_budget_boost_coefficient_df,
    ) = extract.basic()

    if any(
        [
            len(unit_info_df) == 0,
            len(campaign_info_df) == 0,
            len(campaign_all_actual_df) == 0,
            len(ad_info_df) == 0,
            len(ad_target_actual_df) == 0,
        ]
    ):
        raise ValueError(
            "Data does not exists."
            f"[advertising_account_id]{Event.advertising_account_id},"
            f"[portfolio_id]{Event.portfolio_id}"
        )

    logger.info("prepare df")
    target_ad_df, target_campaign_df, target_unit_df = prepare_df.commons(
        unit_info_df=unit_info_df,
        campaign_info_df=campaign_info_df,
        campaign_all_actual_df=campaign_all_actual_df,
        ad_info_df=ad_info_df,
        ad_target_actual_df=ad_target_actual_df,
        daily_budget_boost_coefficient_df=daily_budget_boost_coefficient_df,
    )

    logger.info("extract campaign placement")
    campaign_placement_df = extract.campaign_placement()

    logger.info("cpc_prediction")
    cpc_prediction_df = cpc_prediction.exec(target_ad_df, campaign_placement_df)

    del campaign_placement_df

    logger.info("extract keyword queries")
    keyword_queries_df = prepare_df.add_unit_key(extract.keyword_queries())

    logger.info("cvr_prediction")
    cvr_prediction_df = cvr_prediction.exec(target_ad_df, keyword_queries_df.copy())

    logger.info("spa_prediction")
    spa_prediction_df = spa_prediction.exec(target_ad_df, keyword_queries_df.copy())

    del keyword_queries_df

    # IT時のmock処理
    if it_mock_kpi_predictions is not None:
        cpc_prediction_df, cvr_prediction_df, spa_prediction_df = \
            _generate_it_mock_kpi_predictions(it_mock_kpi_predictions)

    logger.info("target_pause")
    target_pause_df = target_pause.exec(target_unit_df, target_ad_df)

    logger.info("extract lastday ml result")
    lastday_ml_result_unit_df = extract.lastday_ml_result_unit()
    lastday_ml_result_campaign_df = extract.lastday_ml_result_campaign()
    lastday_ml_result_ad_df = extract.lastday_ml_result_ad()

    logger.info("ml_apply")
    is_lastday_ml_applied = ml_apply.exec(ad_target_actual_df, lastday_ml_result_ad_df)

    logger.info("extract ml applied history")
    ml_applied_history_df = None
    if not is_lastday_ml_applied:
        ml_applied_history_df = extract.ml_applied_history()

    logger.info("pid_controller")
    pid_controller_df = pid_controller.exec(
        target_ad_df,
        target_unit_df,
        lastday_ml_result_unit_df,
        is_lastday_ml_applied,
        ml_applied_history_df,
        campaign_all_actual_df,
        cpc_prediction_df,
        cvr_prediction_df,
        spa_prediction_df,
    )

    logger.info("extract ad input json")
    ad_input_json_df = extract.ad_input_json()

    logger.info("bid_optimiser")
    bid_optimiser_df = bid_optimiser.exec(
        target_unit_df,
        target_ad_df,
        ad_input_json_df,
        cpc_prediction_df,
        cvr_prediction_df,
        spa_prediction_df,
        pid_controller_df,
        campaign_all_actual_df,
    )

    logger.info("cap_daily_budget")
    cap_daily_budget_df = cap_daily_budget.exec(
        ad_input_json_df,
        lastday_ml_result_campaign_df,
        lastday_ml_result_unit_df,
        target_campaign_df,
        target_unit_df,
        campaign_info_df,
        campaign_all_actual_df,
        daily_budget_boost_coefficient_df,
    )

    logger.info("output_json")
    output_json.exec(
        ad_input_json_df, bid_optimiser_df, cap_daily_budget_df, target_pause_df
    )

    logger.info("output_csv")
    output_csv.exec(
        target_ad_df,
        target_campaign_df,
        target_unit_df,
        is_lastday_ml_applied,
        cpc_prediction_df,
        cvr_prediction_df,
        spa_prediction_df,
        bid_optimiser_df,
        target_pause_df,
        cap_daily_budget_df,
        pid_controller_df,
        ad_input_json_df,
        lastday_ml_result_unit_df,
    )


def lambda_handler(event, context):
    logger.info(f"start {event}")
    try:
        main(**event)
    except Exception as e:
        raise Exception(
            f"[unit]{event}\n[error]{e}\n[traceback]{traceback.format_exc()}"
        )
    logger.info(f"finished {event}")
