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


    def train_test_split(self):
        if self.loader.table_exists('test') and  self.loader.table_exists('train'):
            train, test = pd.read_sql_query(f"SELECT * FROM  train", con=self.loader.engine),\
                          pd.read_sql_query(f"SELECT * FROM  test", con=self.loader.engine)
        else:
            test = pd.read_sql_query(
                f"""SELECT playlist_primary_id, (tracks_in_playlist::int[])[:cardinality(tracks_in_playlist::int[])*0.8] as tracks_included,
                (tracks_in_playlist::int[])[cardinality(tracks_in_playlist::int[])*0.8:] as tracks_excluded
                from playlist_tracks LIMIT (SELECT COUNT(*)*0.2 FROM  playlist_tracks) OFFSET (SELECT COUNT(*)*0.8 FROM  playlist_tracks)""",
                con=self.loader.engine)
            train = pd.read_sql_query(
                f"""SELECT playlist_primary_id, (tracks_in_playlist::int[]) as tracks_in_playlist
                from playlist_tracks LIMIT (SELECT COUNT(*)*0.8 FROM  playlist_tracks)""", con=self.loader.engine)
            test.to_sql('test', con=self.loader.engine, index=False)
            train.to_sql('train', con=self.loader.engine, index=False, if_exists='replace', chunksize=100000)
        return train, test



    # def predict_similar_for_playlist(self, playlist, n, track):
    #     numeric_cols = ['duration_ms', 'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness',
    #                     'acousticness',
    #                     'instrumentalness', 'liveness', 'valence', 'tempo']
    #     tracks_not_in_playlist = track[~track.track_primary_id.isin(playlist['tracks_in_playlist'])]
    #     X = pd.concat([playlist[numeric_cols].to_frame().T, tracks_not_in_playlist[numeric_cols]])
    #     pipe = Pipeline([('scaler', StandardScaler()), ('imputer', SimpleImputer(strategy='mean'))])
    #     X = pipe.fit_transform(X)
    #     playlist_vals = X[0]
    #     tracks = X[1:]
    #     similarity = [
    #         (cosine_similarity_spatial(playlist_vals, track=tracks[i]), tracks_not_in_playlist.track_primary_id[i]) for
    #         i in range(len(tracks))]
    #     top_n_similar = heapq.nlargest(n, similarity)
    #     return top_n_similar



    def create_pipeline(self):
        pipeline_creator = PipelineCreator(numeric_impute_strategy="mean", categorical_impute_strategy="most_frequent",
                                           numerical_features=self.col_type_double,
                                           categorical_features=self.col_type_string)
        pipeline = pipeline_creator.create()
        return pipeline

