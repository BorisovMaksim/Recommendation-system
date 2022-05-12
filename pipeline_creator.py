from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, MinMaxScaler
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer


class PipelineCreator:
    def __init__(self, numeric_impute_strategy, categorical_impute_strategy,
                 numerical_features, categorical_features):
        self.numeric_impute_strategy = numeric_impute_strategy
        self.categorical_numeric_impute_strategy = categorical_impute_strategy
        self.numerical_features = numerical_features
        self.categorical_features = categorical_features

    def create(self):
        """mean and most_frequent"""
        numeric_pipeline = Pipeline(steps=[
            ('impute', SimpleImputer(strategy=self.numeric_impute_strategy[0])),
            ('scale', MinMaxScaler())
        ])
        categorical_pipeline = Pipeline(steps=[
            ('impute', SimpleImputer(strategy=self.categorical_numeric_impute_strategy[1])),
            ('one-hot', OneHotEncoder(handle_unknown='ignore', sparse=False))
        ])

        full_processor = ColumnTransformer(transformers=[
            ('number', numeric_pipeline, self.numerical_features),
            ('category', categorical_pipeline, self.categorical_features)
        ])
        return full_processor
