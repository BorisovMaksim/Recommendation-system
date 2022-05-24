import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
import heapq
from base_model import BaseModel


def cosine_similarity_numpy(array, vector):
    array_norm = np.linalg.norm(array, axis=1)
    vector_norm = np.linalg.norm(vector)
    return (array @ vector) / (array_norm * vector_norm)


"""r_precision for similarity_model after 2267 iterations = 0.064"""


class SimilarityModel(BaseModel):
    def __init__(self, track=None, playlist_train=None, playlist_test=None):
        self.track = track
        self.playlist_train = playlist_train
        self.playlist_test = playlist_test
        self.numeric_cols = ['duration_ms', 'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness',
                             'acousticness',
                             'instrumentalness', 'liveness', 'valence', 'tempo']
        self.playlist_num_cols = ['avg_' + x for x in self.numeric_cols]

    def train(self):
        pass

    def test(self):
        r_precisions = []
        last_score = -1
        for num, playlist in self.playlist_test.iterrows():
            tracks_not_in_playlist = self.track[~self.track.track_primary_id.isin(
                list(eval(playlist['tracks_included'])))]
            similarity = cosine_similarity_numpy(array=tracks_not_in_playlist[self.numeric_cols].values,
                                                 vector=playlist[self.playlist_num_cols].values)
            top_similar = heapq.nlargest(20000, zip(similarity, tracks_not_in_playlist.track_primary_id.values))
            index_tracks_excluded = list(eval(playlist.tracks_excluded))
            r_precision = len(set([temp_index for temp_playlist, temp_index in top_similar])
                              & set(index_tracks_excluded)) / len(index_tracks_excluded)
            r_precisions.append(r_precision)
            if num % 100 == 0:
                cur_score = np.mean(r_precisions)
                if abs(cur_score - last_score) < 10e-3:
                    break
                last_score = cur_score
            print(
                f"r_precision for similarity_model after {num} iterations = {np.mean(r_precisions)}")
        return np.mean(r_precisions)

    def process_data(self):
        pipe = Pipeline([('scaler', StandardScaler()), ('imputer', SimpleImputer(strategy='mean'))])
        self.track[self.numeric_cols] = pipe.fit_transform(self.track[self.numeric_cols].values)
        self.playlist_test[self.playlist_num_cols] = pipe.fit_transform(
            self.playlist_test[self.playlist_num_cols].values)
