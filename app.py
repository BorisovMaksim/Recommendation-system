import pandas as pd
from data_converter import DataConverter
from data_loader import DataLoader
from data_cleaner import DataCleaner
from similarity_model import SimilarityModel

class App:
    def __init__(self, model_id, stage):
        if stage not in ['creating_csv', 'loading_data', 'downloading_songs', 'loading_model', "train", "test"]:
            raise ValueError("ML Stages are: \n1. creating_csv\n2. loading_data\n3. downloading_songs\n4. loading_model\n5. train\n"
                  "6. test")
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
            if self.model_id == "similarity":
                pass
    def test(self):
        if self.stage == "test":
            if self.model_id == "similarity":
                train, test = self.train_test_split()
                track = pd.read_sql_table('track', con=self.loader.engine)
                model = SimilarityModel(track=track, train=train, playlist_test=test)
                score = model.test()
                print(f"r_precision for similarity_model = {score}")

