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
        playlist_id = pd.read_sql_query("SELECT playlist_primary_id FROM playlist", con=self.loader.engine)
        train_id, test_id = train_test_split(playlist_id, test_size=0.2, random_state=1)
        train_id_str = ", ".join(train_id.values.flatten().astype("str"))
        train_cols = ['duration_ms', 'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness',
                      'instrumentalness', 'liveness', 'valence', 'tempo']
        sub_statement = ", ".join([f"AVG({col}) AS {col}" for col in train_cols])
        playlist_train = pd.read_sql_query(
            f"""SELECT playlist_track_int.playlist_primary_id, 
            array_agg(playlist_track_int.track_primary_id) AS tracks_in_playlist, {sub_statement} FROM 
            track JOIN playlist_track_int 
            ON track.track_primary_id = playlist_track_int.track_primary_id 
            WHERE playlist_track_int.playlist_primary_id IN ({train_id_str}) 
            GROUP BY playlist_track_int.playlist_primary_id;""",
            con=self.loader.engine)

        playlist_test = pd.read_sql_query(
            f""" WITH test_track AS (SELECT * from track ORDER BY random() LIMIT (select count(*)*0.8 from track))
                    SELECT playlist_track_int.playlist_primary_id, 
                    array_agg(playlist_track_int.track_primary_id) AS tracks_in_playlist, {sub_statement} FROM 
                    test_track JOIN playlist_track_int 
                    ON test_track.track_primary_id = playlist_track_int.track_primary_id 
                    WHERE playlist_track_int.playlist_primary_id NOT IN ({train_id_str}) 
                    GROUP BY playlist_track_int.playlist_primary_id;""",
            con=self.loader.engine)
        playlist_train.to_sql("playlist_train", con=self.loader.engine)
        playlist_test.to_sql("playlist_test", con=self.loader.engine)
        return playlist_train, playlist_test



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

