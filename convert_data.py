import pandas as pd
import os
import json
from itertools import chain


def make_csv_files_from_json_files(path="/home/maksim/Documents/ML/Datasets/Spotify_data", n=10):
    filenames = os.listdir(path + "/jsons/")
    for j in range(n):
        playlist_track = pd.DataFrame()
        playlist = pd.DataFrame()
        track = pd.DataFrame()
        for filename in filenames[len(filenames) // n * j:len(filenames) // n * (j + 1)]:
            fullpath = os.sep.join((path + "/jsons/", filename))
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
        playlist_track.to_csv(path + '/playlist_track_{}.csv'.format(j), encoding='utf-8', index=False)
        playlist.to_csv(path + '/playlist_{}.csv'.format(j), encoding='utf-8', index=False)
        track.to_csv(path + '/track_{}.csv'.format(j), encoding='utf-8', index=False)

    df_playlist = pd.concat([pd.read_csv(path + '/playlist_{}.csv'.format(i)) for i in range(n)])
    df_track = pd.concat([pd.read_csv(path + '/track_{}.csv'.format(i)) for i in range(n)])
    df_playlist_track = pd.concat([pd.read_csv(path + '/playlist_track_{}.csv'.format(i)) for i in range(n)])

    df_track_unique = df_track.drop_duplicates('track_uri').drop('pid', axis=1)

    df_playlist.to_csv(path + '/playlist_full.csv', encoding='utf-8', index=False)
    df_track_unique.to_csv(path + '/track_full.csv', encoding='utf-8', index=False)
    df_playlist_track.to_csv(path + '/playlist_track_full.csv', encoding='utf-8', index=False)