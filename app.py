import pandas as pd
from preprocessing.data_converter import DataConverter
from preprocessing.data_loader import DataLoader
from preprocessing.data_cleaner import DataCleaner
from modelling.similarity_model import SimilarityModel
from modelling.base_model import BaseModel
from modelling.random_model import RandomModel

class App:
    def __init__(self, model_name, stage):
        if stage not in ['process_raw_data', 'loading_data_to_db', 'downloading_songs', "train"]:
            raise ValueError(
                "Stages are: \n1. process_raw_data\n2. loading_data_to_db\n3. downloading_songs\n4. train\n")
        if model_name not in ['random','cos_similarity']:
            raise ValueError(
                "Models are: \n1.random\n2. cos_similarity")
        self.model_name = model_name
        self.stage = stage
        self.col_type_tgt = []
        self.col_type_double = []
        self.col_type_string = []
        self.loader = DataLoader()
        self.converter = DataConverter()
        self.models = {"cos_similarity": SimilarityModel, "random": RandomModel}
        self.model = self.models[self.model_name]

    def get_model(self, track, train, test) -> BaseModel:
        return self.model(track=track, playlist_train=train, playlist_test=test)

    def process_raw_data(self):
        if self.stage == "process_raw_data":
            self.converter.make_csv_files_from_json_files()

    def load_data_to_db(self):
        if self.stage == "loading_data_to_db":
            self.loader.load_audio_features_to_db()
            self.loader.load_csv_tables_to_db()
            cleaner = DataCleaner()
            cleaner.clean(retain="songs")

    def download_songs(self, num_playlists=10):
        if self.stage == "downloading_songs":
            self.loader.load_random(num_playlists=num_playlists)

    def train_test_split(self):
        if self.stage == "train" or self.stage == "test":
            if self.loader.table_exists('test') and self.loader.table_exists('train'):
                train, test = pd.read_sql_query(f"SELECT * FROM  train", con=self.loader.engine), \
                              pd.read_sql_query(f"SELECT * FROM  test", con=self.loader.engine)
            else:
                avg_num_cols = ", ".join([f"AVG({x}) AS avg_{x}" for x in
                                          ['duration_ms', 'danceability', 'energy', 'key', 'loudness', 'mode',
                                           'speechiness', 'acousticness',
                                           'instrumentalness', 'liveness', 'valence', 'tempo']])
                train = pd.read_sql_query(f"""
                            WITH train_data AS 
                            (SELECT playlist_primary_id, tracks_in_playlist::int[] as tracks_in_playlist
                            FROM playlist_tracks LIMIT (SELECT COUNT(*)*0.8 FROM  playlist_tracks))
                            SELECT playlist_primary_id, MAX(tracks_in_playlist) AS tracks_in_playlist, {avg_num_cols}
                            FROM track JOIN train_data 
                            ON track.track_primary_id = ANY(train_data.tracks_in_playlist::int[]) 
                            GROUP BY playlist_primary_id
                            """, con=self.loader.engine)
                test = pd.read_sql_query(f"""
                            WITH test_data AS 
                            (SELECT playlist_primary_id,
                             (tracks_in_playlist::int[])[:cardinality(tracks_in_playlist::int[])*0.8] as tracks_included,
                            (tracks_in_playlist::int[])[cardinality(tracks_in_playlist::int[])*0.8:] as tracks_excluded
                            FROM playlist_tracks 
                            LIMIT (SELECT COUNT(*) FROM  playlist_tracks)*0.2 
                            OFFSET (SELECT COUNT(*) FROM  playlist_tracks)*0.8)
    
                            SELECT playlist_primary_id, MAX(tracks_included)  AS tracks_included,
                            MAX(tracks_excluded) AS tracks_excluded , {avg_num_cols}
                            FROM track JOIN test_data 
                            ON track.track_primary_id = ANY(test_data.tracks_included::int[]) 
                            GROUP BY playlist_primary_id;
                            """, con=self.loader.engine)

                test.to_sql('test', con=self.loader.engine, index=False, if_exists='replace', chunksize=100000)
                train.to_sql('train', con=self.loader.engine, index=False, if_exists='replace', chunksize=100000)
            return train, test

    def train(self):
        if self.stage == "train":
            train, test = self.train_test_split()
            track = pd.read_sql_table('track', con=self.loader.engine)
            model = self.get_model(track, train, test)
            model.process_data()
            model.train()
            r_precision = model.test()
            return r_precision
