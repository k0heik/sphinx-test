from .preprocess import CVRPreprocessor
from .estimator import CVREstimator


class CVRPredictionService:

    def __init__(self, preprocessor: CVRPreprocessor, estimator: CVREstimator):
        self._preprocessor = preprocessor
        self._estimator = estimator

    def train(self, df, df_query):
        df = self._preprocessor.preprocess(df, df_query, True)
        self._estimator.fit(df)

    def predict(self, df, df_query, today):
        df = self._preprocessor.preprocess(df, df_query, False)
        df = self._estimator.predict(df, today)
        return df
