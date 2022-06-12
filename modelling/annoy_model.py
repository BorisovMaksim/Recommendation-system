import os.path

from sklearn.model_selection import train_test_split
import numpy as np
from modelling.base_model import BaseModel
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from annoy import AnnoyIndex
from sklearn.impute import SimpleImputer

"""r_precision for annoy_model after 1118 iterations = 0.066"""


class AnnoyModel(BaseModel):
    def __init__(self, data):
        self.data = data
        self.dim = data.shape[1]

    def train(self):
        X = self.data[self.data.columns.difference(['tracks_included', 'tracks_excluded'])]
        scaler = StandardScaler()
        X[X.columns] = scaler.fit_transform(X)
        X_train, X_test = train_test_split(X, test_size=0.33, random_state=42)
        if os.path.exists('/home/maksim/Data/Spotify/annoy_playlist.ann'):
            t = AnnoyIndex(self.dim, metric='angular')
            for index, playlist in X_train.iterrows():
                t.add_item(index, playlist)
            t.build(n_trees=10)
            t.save('/home/maksim/Data/Spotify/annoy_playlist.ann')
        # u = AnnoyIndex(self.dim, metric='angular')
        # u.load('/home/maksim/Data/Spotify/annoy_playlist.ann')
        #
        # r_precisions = []
        # last_score = -1
        # for index, playlist in X_test.iterrows():
        #     top_similar = u.get_nns_by_vector(playlist, 100)
        #     top_similar_not_in_playlist = [x for x in top_similar
        #                                    if x not in list(eval(playlist['tracks_included']))][:500]
        #     index_tracks_excluded = list(eval(playlist.tracks_excluded))
        #     r_precision = len(set(top_similar_not_in_playlist)
        #                       & set(index_tracks_excluded)) / len(index_tracks_excluded)
        #     r_precisions.append(r_precision)
        #     if index % 100 == 0:
        #         cur_score = np.mean(r_precisions)
        #         if abs(cur_score - last_score) < 10e-5:
        #             break
        #         last_score = cur_score
        #     print(f"r_precision for annoy_model after {index} iterations = {np.mean(r_precisions)}")
        # return np.mean(r_precisions)

    def test(self):
        u = AnnoyIndex(self.dim, metric='angular')
        u.load('/home/maksim/Data/Spotify/annoy_playlist.ann')
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
