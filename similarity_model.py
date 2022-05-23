import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
import heapq
from scipy import spatial


def cosine_similarity_spatial(playlist, track):
    return 1 - spatial.distance.cosine(playlist, track)


class SimilarityModel:
    def __init__(self, track, train, playlist_test, numeric_cols=None):
        if numeric_cols is None:
            numeric_cols = ['duration_ms', 'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness',
                            'acousticness',
                            'instrumentalness', 'liveness', 'valence', 'tempo']
        self.numeric_cols = numeric_cols
        self.playlist_num_cols = ['avg_' + x for x in self.numeric_cols]
        self.track = track
        self.train = train
        self.playlist_test = playlist_test

    def test(self):
        self.process_data()
        track_num = self.track[self.numeric_cols].values
        r_precision_s = []
        for _, playlist in self.playlist_test.iterrows():
            tracks_not_in_playlist = self.track[~self.track.track_primary_id.isin(
                list(eval(playlist['tracks_included'])))]
            similarity = [(cosine_similarity_spatial(playlist[self.playlist_num_cols].values,
                                                     tracks_not_in_playlist[self.numeric_cols].values[i]),
                           tracks_not_in_playlist.track_primary_id[i]) for i in range(len(track_num))]
            top_similar = heapq.nlargest(500, similarity)
            r_precision = len(set([x[1] for x in top_similar])
                              & set(playlist.tracks_excluded)) / len(playlist.tracks_excluded)
            r_precision_s.append(r_precision)
        return np.mean(r_precision_s)

    def process_data(self):
        pipe = Pipeline([('scaler', StandardScaler()), ('imputer', SimpleImputer(strategy='mean'))])
        self.track[self.numeric_cols] = pipe.fit_transform(self.track[self.numeric_cols])
        self.playlist_test[self.playlist_num_cols] = pipe.fit_transform(self.playlist_test[self.playlist_num_cols])

