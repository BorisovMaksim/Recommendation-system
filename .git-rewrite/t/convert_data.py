import pandas as pd
import os
import json
from itertools import chain
from myconstants import SPOTIFY_DATA_PATH, SPOTIFY_JSONS_PATH


def make_csv_files_from_json_files(n=10):
    filenames = os.listdir(SPOTIFY_JSONS_PATH)
    for j in range(n):
        print(f"{j} CHUNKS OUT OF {n} PROCESSED")
        playlist_track = pd.DataFrame()
        playlist = pd.DataFrame()
        track = pd.DataFrame()
        for filename in filenames[len(filenames) // n * j:len(filenames) // n * (j + 1)]:
            fullpath = os.sep.join((SPOTIFY_JSONS_PATH, filename))
            f = open(fullpath)
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

        make_csv_files([playlist_track, playlist, track], ['playlist_track', 'playlist', 'track'], j)

    df_playlist, df_track, df_playlist_track = concatenate_csv_files(['playlist', 'track', 'playlist_track'], n)
    df_track_unique = df_track.drop_duplicates('track_uri').drop('pid', axis=1)
    make_csv_files([df_playlist, df_track_unique, df_playlist_track], ['playlist', 'track', 'playlist_track'], 'full')
    delete_csv_files(['playlist_track', 'playlist', 'track'], n)


def make_csv_files(dfs, names, end):
    for i in range(len(names)):
        dfs[i].to_csv(SPOTIFY_DATA_PATH + f'/{names[i]}_{end}.csv', encoding='utf-8', index=False)


def concatenate_csv_files(names, n):
    for name in names:
        yield pd.concat([pd.read_csv(SPOTIFY_DATA_PATH + f'/{name}_{i}.csv') for i in range(n)])


def delete_csv_files(names, n):
    for name in names:
        for i in range(n):
            temp = SPOTIFY_DATA_PATH + f'/{name}_{i}.csv'
            if os.path.isfile(temp):
                os.remove(temp)
