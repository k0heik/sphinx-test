from .preprocess import CAPPreprocessor
from .calculator import CAPCalculator


class CAPCalculationService:

    def __init__(self, preprocessor: CAPPreprocessor, calculator: CAPCalculator):
        self._preprocessor = preprocessor
        self._calculator = calculator

    def calc(self, df, daily_df, campaign_all_actual_df):
        df, daily_df = self._preprocessor.transform(df, daily_df)
        return self._calculator.calc(df, daily_df, campaign_all_actual_df)
