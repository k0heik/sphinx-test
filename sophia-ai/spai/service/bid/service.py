from .preprocess import BIDPreprocessor
from .calculator import BIDCalculator
from .config import OUTPUT_DTYPES


class BIDCalculationService:

    def __init__(self, preprocessor: BIDPreprocessor, calculator: BIDCalculator):
        self._preprocessor = preprocessor
        self._calculator = calculator

    def calc(
        self,
        df,
        campaign_all_actual_df,
        bidding_algorithm,
        advertising_account_id: int = None,
        portfolio_id: int = None
    ):
        bid_configs = self._preprocessor.transform(df, advertising_account_id, portfolio_id)
        return self._calculator.calc(
            bid_configs, campaign_all_actual_df, bidding_algorithm)[OUTPUT_DTYPES.keys()]
