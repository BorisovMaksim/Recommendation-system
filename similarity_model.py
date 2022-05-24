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


def cosine_similarity_numpy(array, vector):
    array_norm = np.linalg.norm(array, axis=1)
    vector_norm = np.linalg.norm(vector)
    return (array @ vector) / (array_norm * vector_norm)


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
        r_precisions = []
        j = 0
        for index, playlist in self.playlist_test.iterrows():
            tracks_not_in_playlist = self.track[~self.track.track_primary_id.isin(
                list(eval(playlist['tracks_included'])))]
            similarity = cosine_similarity_numpy(array=tracks_not_in_playlist[self.numeric_cols].values,
                                                 vector=playlist[self.playlist_num_cols].values)
            top_similar = heapq.nlargest(20000, zip(similarity, tracks_not_in_playlist.track_primary_id.values))
            index_tracks_excluded = list(eval(playlist.tracks_excluded))
            r_precision = len(set([temp_index for temp_playlist, temp_index in top_similar])
                              & set(index_tracks_excluded)) / len(index_tracks_excluded)
            r_precisions.append(r_precision)
            print(
                f"r_precision for similarity_model after {j} iterations = {np.mean(r_precisions)}, where index={index}")
            j += 1
        return np.mean(r_precisions)

    def process_data(self):
        pipe = Pipeline([('scaler', StandardScaler()), ('imputer', SimpleImputer(strategy='mean'))])
        self.track[self.numeric_cols] = pipe.fit_transform(self.track[self.numeric_cols].values)
        self.playlist_test[self.playlist_num_cols] = pipe.fit_transform(self.playlist_test[self.playlist_num_cols].values)
