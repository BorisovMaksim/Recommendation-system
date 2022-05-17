import os
import pandas as pd
import json
from itertools import chain
from config import my_config


def process_json(filename, playlist_track, track, playlist):
    f = open(os.path.abspath(filename))
    js = f.read()
    f.close()
    mpd_slice = json.loads(js)
    playlist_temp = pd.DataFrame(mpd_slice['playlists'])
    playlist_temp['tracks'] = pd.Series([[playlist_temp.tracks[j][i] | {'pid': playlist_temp.pid[j]}
                                          for i in range(len(playlist_temp.tracks[j]))]
                                         for j in range(len(playlist_temp))])
    track_temp = pd.DataFrame(list(chain.from_iterable(playlist_temp['tracks'])))

    playlist_track = pd.concat([playlist_track, track_temp[['pid', 'track_uri']]])
    track = pd.concat([track, track_temp]).drop_duplicates('track_uri')
    playlist = pd.concat([playlist, playlist_temp.drop(columns=['tracks'], axis=1)])
    return playlist_track, track, playlist


class DataConverter:
    def __init__(self):
        self.root_dir = my_config['SPOTIFY']['DATA_PATH']
        self.jsons_dir = os.path.join(self.root_dir, "data/")

    def make_csv_files_from_json_files(self, chunk_size=10):
        filenames = os.listdir(self.jsons_dir)
        for chunk_num in range(chunk_size):
            print(f"{chunk_num} CHUNKS OUT OF {chunk_size} PROCESSED")
            playlist_track = pd.DataFrame()
            playlist = pd.DataFrame()
            track = pd.DataFrame()
            chunk_left_border = len(filenames) // chunk_size * chunk_num
            chunk_right_border = chunk_left_border + len(filenames) // chunk_size
            for filename in filenames[chunk_left_border:chunk_right_border]:
                playlist_track, track, playlist = process_json(filename, playlist_track, track, playlist)
            if self.make_csv_files([playlist_track, playlist, track], ['playlist_track', 'playlist', 'track'],
                                   chunk_num):
                print(f"CSV FILES FROM CHUNK {chunk_num} ARE SAVED AT {self.root_dir}")
        df_playlist, df_track, df_playlist_track = self.concatenate_csv_files(['playlist', 'track', 'playlist_track'],
                                                                              chunk_size)
        df_track_unique = df_track.drop_duplicates('track_uri').drop('pid', axis=1)
        if self.make_csv_files([df_playlist, df_track_unique, df_playlist_track],
                               ['playlist', 'track', 'playlist_track'], 'full'):
            print(f"ALL CSV FILES ARE SAVED AT {self.root_dir}")
        if self.delete_csv_files(['playlist_track', 'playlist', 'track'], chunk_size):
            print("TEMPORARY CSV FILES ARE DELETED")
        return 1

    def make_csv_files(self, dfs, names, end):
        for i in range(len(names)):
            dfs[i].to_csv(os.path.join(self.root_dir, f'{names[i]}_{end}.csv'), encoding='utf-8', index=False)
        return 1

    def concatenate_csv_files(self, names, n):
        for name in names:
            yield pd.concat([pd.read_csv(os.path.join(self.root_dir, f'{name}_{i}.csv')) for i in range(n)])

    def delete_csv_files(self, names, n):
        for name in names:
            for i in range(n):
                temp = os.path.join(self.root_dir, f'{name}_{i}.csv')
                if os.path.isfile(temp):
                    os.remove(temp)
        return 1
