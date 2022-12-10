from .preprocess import CPCPreprocessor
from .estimator import CPCEstimator


class CPCPredictionService:

    def __init__(self, preprocessor: CPCPreprocessor, estimator: CPCEstimator):
        self._preprocessor = preprocessor
        self._estimator = estimator

    def train(self, df, df_placement):
        df = self._preprocessor.preprocess(df, df_placement, is_train=True)
        self._estimator.fit(df)

    def predict(self, df, df_placement, today):
        df = self._preprocessor.preprocess(df, df_placement, is_train=False)
        df = self._estimator.predict(df, today)
        return df
