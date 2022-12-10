from .preprocess import PIDPreprocessor
from .calculator import PIDCalculator


class PIDCalculationService:

    def __init__(self, preprocessor: PIDPreprocessor, calculator: PIDCalculator):
        self._preprocessor = preprocessor
        self._calculator = calculator

    def calc(self, df, campaign_all_actual_df):
        df = self._preprocessor.transform(df)
        return self._calculator.calc(df, campaign_all_actual_df)
