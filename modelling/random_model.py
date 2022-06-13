import numpy as np

"""r_precision for random_model after 199 iterations = 0.008"""


class RandomModel:
    def __init__(self, data, engine):
        self.engine = engine
        self.track = data
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
            index_random_tracks = tracks_not_in_playlist.track_primary_id.sample(n=20000).values
            index_tracks_excluded = list(eval(playlist.tracks_excluded))
            r_precision = len(set(index_random_tracks) & set(index_tracks_excluded)) / len(index_tracks_excluded)
            r_precisions.append(r_precision)
            if num % 100 == 0:
                cur_score = np.mean(r_precisions)
                if abs(cur_score - last_score) < 10e-5:
                    break
                last_score = cur_score
            print(
                f"r_precision for random_model after {num} iterations = {np.mean(r_precisions)}")
        return np.mean(r_precisions)

    def process_data(self):
        pass
