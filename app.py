import pandas as pd
from preprocessing.data_loader import DataLoader
from modelling.similarity_model import SimilarityModel
from modelling.base_model import BaseModel
from modelling.random_model import RandomModel
from modelling.annoy_model import AnnoyModel


class App:
    def __init__(self, model_name):

        if model_name not in ['random', 'cos_similarity', 'annoy']:
            raise ValueError("Models are: \n1.random\n2. cos_similarity")
        self.model_name = model_name
        self.col_type_tgt = []
        self.col_type_double = []
        self.col_type_string = []
        self.loader = DataLoader()
        self.models = {"cos_similarity": SimilarityModel, "random": RandomModel, "annoy": AnnoyModel}
        self.model = self.models[self.model_name]

    def get_model(self, track, train, test) -> BaseModel:
        return self.model(track=track, playlist_train=train, playlist_test=test)

    def collect_data(self):
        self.loader.load_data_to_db()
        self.loader.update_db()
        self.loader.download_audio_features()

    def extract_features(self):
        avg_num_cols = ", ".join([f"AVG(track.{x}) AS avg_{x}" for x in
                                  ['duration_ms', 'danceability', 'energy', 'key', 'loudness', 'mode', 'tempo',
                                   'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence']])
        data = pd.read_sql_query(f""" WITH temp_playlist AS (SELECT * FROM playlist)
                                SELECT temp_playlist.id, 
                                (array_agg(track.id)::int[])[:cardinality(array_agg(track.id))*0.8] as tracks_included,
                                (array_agg(track.id)::int[])[cardinality(array_agg(track.id))*0.8:] as tracks_excluded,
                                {avg_num_cols},
                                COUNT(DISTINCT track.id) as num_tracks, COUNT(DISTINCT track.artist_uri) as num_artists
                                FROM temp_playlist
                                LEFT JOIN playlist_track 
                                ON temp_playlist.id = playlist_track.playlist_id
                                LEFT JOIN track
                                ON playlist_track.track_id = track.id
                                GROUP BY  temp_playlist.id;
                                """, con=self.loader.engine)
        data.to_pickle("./data.pkl")

    def download_songs(self, num_playlists=10):
        self.loader.load_random(num_playlists=num_playlists)


    def train(self):
        train, test = self.train_test_split()
        track = pd.read_sql_table('track', con=self.loader.engine)
        model = self.get_model(track, train, test)
        model.process_data()
        model.train()
        r_precision = model.test()
        return r_precision


