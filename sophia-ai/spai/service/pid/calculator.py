import numpy as np
import pandas as pd

from spai.optim.pid import calc_states
from spai.optim.models import KPI, Purpose, \
    Settings, State, PIDConfig
from spai.utils.logger import get_custom_logger
from .config import ML_LOOKUP_DAYS, OUTPUT_COLUMNS


logger = get_custom_logger(__name__)


class PIDCalculator():

    def __init__(self, today) -> None:
        self._today = today

    def _skip_calc_state_row(self, df):
        row = df.iloc[-1].to_dict()
        row["is_skip_pid_calc_state"] = True

        keys = row.keys()
        for col in OUTPUT_COLUMNS:
            if col not in keys:
                row[col] = None

        return row

    def map_pid_calc(self, args):
        unit_id = args['unit_id']
        logger.info(f"start pid_calc unit_id: {unit_id}")

        today = args['today']
        pid_config = args['pid_config']
        df = args['df']
        campaign_all_actual_df = args['campaign_all_actual_df']

        df = df.sort_values(['ad_id', 'date']).reset_index()

        if df['bidding_price'].isna().all():
            logger.warning(
                f"""All bid prices are invalid.
                The output of {unit_id} is not created"""
            )
            return None

        if df["target_cost"].values[-1] == 0:
            logger.warning(
                f"""target_cost is zero.
                The output of {unit_id} is same yesterday """)
            return self._skip_calc_state_row(df)

        (
            settings,
            p_state,
            q_state,
            pre_reupdate_p_state,
            pre_reupdate_q_state,
            is_updated,
            is_pid_initialized,
            obs_kpi,
            valid_ads_num
        ) = calc_states(df, campaign_all_actual_df, today, pid_config)

        if p_state is None:
            logger.warning(
                f"""p is not well initialized.
                The output of {unit_id} is not created """)
            return None

        is_error = self._is_abnormal(p_state, q_state)

        row = self._get_row(
            settings,
            p_state,
            q_state,
            pre_reupdate_p_state,
            pre_reupdate_q_state,
            is_error, is_updated,
            is_pid_initialized,
            obs_kpi,
            valid_ads_num
        )
        row['unit_id'] = unit_id
        row['date'] = today
        row["is_skip_pid_calc_state"] = False
        logger.info(f"complete pid_calc unit_id: {unit_id}")
        return row

    def calc(
        self,
        _df: pd.DataFrame,
        campaign_all_actual_df: pd.DataFrame,
    ) -> pd.DataFrame:
        df = _df.copy()
        df['date'] = pd.to_datetime(df['date'])
        df['unit_id'] = np.where(pd.isnull(df['portfolio_id']),
                                 'a_' + df['advertising_account_id'].astype(str),
                                 'p_' + df['portfolio_id'].apply(str))

        results = list()
        pid_config = PIDConfig(not_ml_applied_days_threshold=ML_LOOKUP_DAYS)
        results = [
            self.map_pid_calc({
                'today': self._today,
                'pid_config': pid_config,
                'unit_id': unit_id,
                'df': gf,
                'campaign_all_actual_df': campaign_all_actual_df,
            }) for unit_id, gf in df.groupby('unit_id')
        ]
        results = [result for result in results if result is not None]

        if len(results) > 0:
            results = pd.DataFrame.from_dict(results)
            results = pd.merge(
                results,
                df[
                    ['advertising_account_id', 'portfolio_id', 'unit_id']
                ].set_index('unit_id').drop_duplicates().reset_index(),
                on='unit_id', how='inner', suffixes=["", "_y"]
            ).drop('unit_id', axis=1)
            results["portfolio_id"] = results["portfolio_id"].astype("Int64")
            return results[OUTPUT_COLUMNS]
        else:
            return None

    def _is_abnormal(self, p_state: State, q_state: State) -> bool:
        """異常値判定を行う。
        pが0だと，永遠に0となってしまうため異常値と判定する
        valid_adsが0の場合は初期化処理をスキップするため，p_state.outputがnullでも通常の処理を続ける"""
        if p_state.output is None:
            return False
        if abs(p_state.output) <= 1e-10:
            return True
        return False

    def _get_row(
        self, s: Settings,
        p: State,
        q: State,
        pre_reupdate_p: State,
        pre_reupdate_q: State,
        is_error: bool,
        is_updated: bool,
        is_pid_initialized: bool,
        obs_kpi: float,
        valid_ads_num: int
    ):
        def purpose2str(purpose: Purpose):
            if purpose is Purpose.SALES:
                return 'SALES'
            elif purpose is Purpose.CONVERSION:
                return 'CONVERSION'
            elif purpose is Purpose.CLICK:
                return 'CLICK'

        def kpi2str(kpi: KPI):
            if kpi is KPI.NULL:
                return 'NULL'
            elif kpi is KPI.ROAS:
                return 'ROAS'
            elif kpi is KPI.CPC:
                return 'CPC'
            elif kpi is KPI.CPA:
                return 'CPA'
        use_q_flag = q is None
        return {
            'purpose': purpose2str(s.purpose),
            'target_kpi': kpi2str(s.kpi),
            'target_kpi_value': s._target_kpi_value,
            'target_cost': s.target_cost,
            'base_target_cost': s.base_target_cost,
            'p': p.output,
            'pre_reupdate_p': pre_reupdate_p.output,
            'p_kp': p.kp,
            'p_ki': p.ki,
            'p_kd': p.kd,
            'p_error': p.error,
            'p_sum_error': p.sum_error,
            'q': None if use_q_flag else q.output,
            'pre_reupdate_q':  None if use_q_flag else pre_reupdate_q.output,
            'q_kp': None if use_q_flag else q.kp,
            'q_ki': None if use_q_flag else q.ki,
            'q_kd': None if use_q_flag else q.kd,
            'q_error': None if use_q_flag else q.error,
            'q_sum_error': None if use_q_flag else q.sum_error,
            'origin_p': p.original_output,
            'origin_q': None if use_q_flag else q.original_output,
            'error': is_error,
            'is_updated': is_updated,
            'is_pid_initialized': is_pid_initialized,
            'obs_kpi': obs_kpi,
            'valid_ads_num': valid_ads_num,
        }
