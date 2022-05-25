import os.path

import numpy as np
from modelling.base_model import BaseModel
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from annoy import AnnoyIndex
from sklearn.impute import SimpleImputer

"""r_precision for annoy_model after 200 iterations = 0.78"""


class AnnoyModel(BaseModel):
    def __init__(self, track=None, playlist_train=None, playlist_test=None):
        self.track = track
        self.playlist_train = playlist_train
        self.playlist_test = playlist_test
        self.numeric_cols = ['duration_ms', 'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness',
                             'acousticness',
                             'instrumentalness', 'liveness', 'valence', 'tempo']
        self.playlist_num_cols = ['avg_' + x for x in self.numeric_cols]
        self.dim = len(self.numeric_cols)

    def train(self):
        if os.path.exists('/home/maksim/Data/Spotify/test.ann'):
            return
        t = AnnoyIndex(self.dim, metric='angular')
        for num, track in self.track.iterrows():
            if num % 10000 == 0:
                print(f"train iteration = {num}")
            vector = track[self.numeric_cols]
            index = track.track_primary_id
            t.add_item(index, vector)
        t.build(n_trees=10)
        t.save('/home/maksim/Data/Spotify/test.ann')

    def test(self):
        u = AnnoyIndex(self.dim, metric='angular')
        u.load('/home/maksim/Data/Spotify/test.ann')
        r_precisions = []
        last_score = -1
        for num, playlist in self.playlist_test.iterrows():
            top_similar = u.get_nns_by_vector(playlist[self.playlist_num_cols].values, 25000)
            top_similar_not_in_playlist = [x for x in top_similar
                                           if x not in list(eval(playlist['tracks_included']))][:20000]
            index_tracks_excluded = list(eval(playlist.tracks_excluded))
            r_precision = len(set(top_similar_not_in_playlist)
                              & set(index_tracks_excluded)) / len(index_tracks_excluded)
            r_precisions.append(r_precision)
            if num % 100 == 0:
                cur_score = np.mean(r_precisions)
                if abs(cur_score - last_score) < 10e-5:
                    break
                last_score = cur_score
            print(f"r_precision for annoy_model after {num} iterations = {np.mean(r_precisions)}")
        return np.mean(r_precisions)

    def process_data(self):
        pipe = Pipeline([('scaler', StandardScaler()), ('imputer', SimpleImputer(strategy='mean'))])
        self.track[self.numeric_cols] = pipe.fit_transform(self.track[self.numeric_cols].values)
        self.playlist_test[self.playlist_num_cols] = pipe.fit_transform(
            self.playlist_test[self.playlist_num_cols].values)
