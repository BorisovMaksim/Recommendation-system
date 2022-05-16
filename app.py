import pandas as pd

from data_converter import DataConverter
from data_loader import DataLoader
from data_cleaner import DataCleaner
from pipeline_creator import PipelineCreator
import numpy as np
from sklearn.model_selection import train_test_split


class App:
    def __init__(self, model_id, stage):
        if stage not in ['creating_csv', 'loading_data', 'downloading_songs', 'loading_model']:
            print("ML Stages are: \n1. creating_csv\n2. loading_data\n3. loading_songs\n4. loading_model\n")
        self.model_id = model_id
        self.stage = stage
        self.col_type_tgt = []
        self.col_type_double = []
        self.col_type_string = []
        self.loader = DataLoader()
        self.converter = DataConverter()

    def process_raw_data(self):
        if self.stage == "creating_csv":
            self.converter.make_csv_files_from_json_files()

    def load_data_to_db(self):
        if self.stage == "loading_data":
            self.loader.load_audio_features_to_db()
            self.loader.load_csv_tables_to_db()
            cleaner = DataCleaner()
            cleaner.clean(retain="songs")

    def download_songs(self, num_playlists=10):
        if self.stage == "downloading_songs":
            self.loader.load_random(num_playlists=num_playlists)



    # def extract_model_cols(self, df):
    #     self.col_type_double = df.select_dtypes(include='number').columns
    #     self.col_type_string = df.select_dtypes(include='object').columns

    def train(self):
        # track = self.loader.load_track_data()
        playlist_id = self.loader.load_playlist_data()
        playlist_train, playlist_test = train_test_split(playlist_id, test_size=0.2, random_state=1)
        playlist_train, playlist_validation = train_test_split(playlist_train, test_size=0.25, random_state=1)
        playlist_train_string = [str(x) for x in playlist_train]
        track_train = pd.read_sql_query(f"""SELECT * FROM track LEFT JOIN playlist_track_int ON 
        track.track_primary_id = playlist_track_int.track_primary_id WHERE playlist_track_int.playlist_primary_id 
                            IN ({ ', '.join(playlist_train_string)})""", con=self.loader.engine)
        print(track_train)

    def create_pipeline(self):
        pipeline_creator = PipelineCreator(numeric_impute_strategy="mean", categorical_impute_strategy="most_frequent",
                                           numerical_features=self.col_type_double,
                                           categorical_features=self.col_type_string)
        pipeline = pipeline_creator.create()
        return pipeline
    #
    # def train_test_split(self, df, split_size=0.8):
    #     is_train = df.pid in np.random.permutation(df.pid)[:len(df.pid)*split_size]
    #     train = df[is_train]
    #     test = df[not is_train]
    #     return train, test
    #
    # def fit(self, dataset):
    #     pipeline = self.create_pipeline()
    #     dataset = pipeline.fit_transform(dataset)
    #     return dataset

