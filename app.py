from data_converter import DataConverter
from data_loader import DataLoader
from data_cleaner import DataCleaner
from pipeline_creator import PipelineCreator
import numpy as np


class App:
    def __init__(self, model_id, stage):
        self.model_id = model_id
        self.stage = stage
        # self.pipeline_ensemble = Pipeline() My_pipeline
        self.col_type_tgt = []
        self.col_type_double = []
        self.col_type_string = []
        self.loader = DataLoader()
        self.converter = DataConverter()

    def create_premodel_data(self, num_playlists=10):
        if self.stage == "creating_csv":
            self.converter.make_csv_files_from_json_files()
        elif self.stage == "loading_data":
            self.loader.load_audio_features_to_db()
            self.loader.load_csv_tables_to_db()
            cleaner = DataCleaner()
            cleaner.clean(retain="songs")
        elif self.stage == "loading_songs":
            self.loader.load_random(num_playlists=num_playlists)
        else:
            print("ML Stages are: \n1. creating_csv\n2. loading_data\n3. loading_songs\n4. loading_model\n")

    def load_premodel_data(self):
        df = self.loader.load_dataframe_from_db()
        self.extract_model_cols(df)
        return df

    def extract_model_cols(self, df):
        self.col_type_double = df.select_dtypes(include='number').columns
        self.col_type_string = df.select_dtypes(include='object').columns

    def create_pipeline(self):
        pipeline_creator = PipelineCreator(numeric_impute_strategy="mean", categorical_impute_strategy="most_frequent",
                                           numerical_features=self.col_type_double,
                                           categorical_features=self.col_type_string)
        pipeline = pipeline_creator.create()
        return pipeline

    def train_test_split(self, df, split_size=0.8):
        is_train = df.pid in np.random.permutation(df.pid)[:len(df.pid)*split_size]
        train = df[is_train]
        test = df[not is_train]
        return train, test

    def fit(self, dataset):
        pipeline = self.create_pipeline()
        dataset = pipeline.fit_transform(dataset)
        return dataset

