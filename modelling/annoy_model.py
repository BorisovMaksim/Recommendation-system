import os.path
import pandas as pd
from sklearn.model_selection import train_test_split
import numpy as np
from sklearn.preprocessing import StandardScaler
from annoy import AnnoyIndex

"""r_precision for annoy_model = 0.26"""


class AnnoyModel:
    def __init__(self, data, engine):
        self.data = data
        self.engine = engine
        self.dim = data.select_dtypes(include=['int16', 'int32', 'int64', 'float16', 'float32', 'float64']).shape[1]
        self.num_columns = self.data.columns.difference(['tracks_included', 'tracks_excluded'])
        self.scaler = StandardScaler()

    def get_similar_tracks(self, similar_playlists):
        playlist_track_similar = pd.read_sql_query(
            f"SELECT playlist_id, track_id "
            f"FROM playlist_track "
            f"WHERE playlist_track.playlist_id IN ({', '.join([str(x) for x in similar_playlists])})",
            con=self.engine)
        track_similar = pd.DataFrame(similar_playlists, columns=['playlist_id']).merge(
            playlist_track_similar, how='left', on='playlist_id').track_id
        return track_similar

    def process_data(self):
        self.data[self.num_columns] = self.scaler.fit_transform(self.data[self.num_columns])

    def test(self):
        X_train, X_test = train_test_split(self.data, test_size=0.33, random_state=42)
        if not os.path.exists('./annoy_playlist.ann'):
            t = AnnoyIndex(self.dim, metric='angular')
            for index, playlist in X_train.iterrows():
                t.add_item(index, playlist[self.num_columns])
            t.build(n_trees=10)
            t.save('./annoy_playlist.ann')
        u = AnnoyIndex(self.dim, metric='angular')
        u.load('./annoy_playlist.ann')

        r_precisions = []
        last_score = -1
        for num, (index, playlist) in enumerate(X_test.iterrows()):
            similar_playlists = u.get_nns_by_vector(playlist[self.num_columns], 100)
            similar_tracks = self.get_similar_tracks(similar_playlists=similar_playlists)
            similar_tracks_not_in_playlist = [x for x in similar_tracks
                                                 if x not in playlist['tracks_included']][:500]
            r_precision = len(set(similar_tracks_not_in_playlist)
                              & set(playlist.tracks_excluded)) / len(playlist.tracks_excluded)
            r_precisions.append(r_precision)
            if num % 10 == 0:
                cur_score = np.mean(r_precisions)
                if abs(cur_score - last_score) < 10e-6:
                    break
                last_score = cur_score
            print(f"r_precision for annoy_model after {num} iterations = {np.mean(r_precisions)}")
        return np.mean(r_precisions)

    def train(self):
        if not os.path.exists('./annoy_full_data.ann'):
            t = AnnoyIndex(self.dim, metric='angular')
            for index, playlist in self.data.iterrows():
                t.add_item(index, playlist[self.num_columns])
            t.build(n_trees=10)
            t.save('./annoy_full_data.ann')

    def predict(self, tracks_uri, n):
        u = AnnoyIndex(self.dim, metric='angular')
        u.load('./annoy_full_data.ann')
        audio_cols = [x[4:] for x in self.num_columns if x[:4] == 'avg_']
        df_track = pd.read_sql_query(f"""SELECT {", ".join(audio_cols)},  
                                         artist_uri, uri, id
                                         FROM track 
                                         WHERE uri IN ({', '.join([ f"'{x}'" for x in tracks_uri])})""", con=self.engine)
        df_track['num_artists'] = len(df_track.artist_uri.unique())
        df_track['num_tracks'] = len(df_track)
        playlist = self.scaler.fit_transform(df_track[df_track.columns.difference(['artist_uri', 'uri', 'id'])]).sum(axis=0)
        similar_playlists = u.get_nns_by_vector(playlist, 100)
        similar_tracks_id = self.get_similar_tracks(similar_playlists=similar_playlists)[:n]
        similar_tracks = pd.read_sql_query(f"""SELECT uri FROM track WHERE id IN ({",".join([str(x) for x in
                                                                            similar_tracks_id])}) """, con=self.engine)
        return similar_tracks

