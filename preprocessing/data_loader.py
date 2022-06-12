import subprocess
import re
import spotipy
import os
from spotipy.oauth2 import SpotifyOAuth
import shutil
import pandas as pd
from sqlalchemy import create_engine, inspect
import requests
import time
from config import my_config
import tarfile
import json
import logging



def process_json(js):
    playlist_df = pd.DataFrame(js['playlists'])
    track_df = pd.json_normalize(playlist_df['tracks'].explode('tracks')).drop('pos', axis=1).drop_duplicates(
        subset='track_uri')
    playlist_track_df = pd.concat([playlist_df['pid'],
                                   playlist_df.tracks.apply(lambda row:
                                                            [[playlist['track_uri'], playlist['pos']]
                                                             for playlist in row])], axis=1).explode('tracks')
    playlist_track_df = pd.concat([playlist_track_df.pid.reset_index(drop=True),
                                   pd.DataFrame(playlist_track_df.tracks.to_list(), columns=['tracks', 'pos'])],
                                  axis=1)
    playlist_df = playlist_df.drop('tracks', axis=1)
    return playlist_df, playlist_track_df, track_df


class DataLoader:
    def __init__(self):
        self.path_tar = my_config['SPOTIFY']['DATA_PATH']
        # self.song_directory = os.path.join(self.root_dir, "songs/")
        self.engine = create_engine(f"postgresql+psycopg2://{my_config['DATABASE']['USERNAME']}:"
                                    f"{my_config['DATABASE']['PASSWORD']}@{my_config['DATABASE']['HOST']}:"
                                    f"{my_config['DATABASE']['PORT']}/{my_config['DATABASE']['DB']}")
        self.sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=my_config['SPOTIFY']['CLIENT_ID'],
                                                            client_secret=my_config['SPOTIFY']['CLIENT_SECRET'],
                                                            redirect_uri=my_config['SPOTIFY']['REDIRECT_URI'],
                                                            scope="user-library-read"))

    def table_exists(self, name):
        inspector = inspect(self.engine)
        return inspector.dialect.has_table(self.engine.connect(), name)

    def columns_exists(self, table_name, column_name):
        inspector = inspect(self.engine)
        columns = inspector.get_columns(table_name)
        return any(c["name"] == column_name for c in columns)

    def load_data_to_db(self):
        if self.table_exists('playlist'):
            return
        con = self.engine.connect()
        tar = tarfile.open(self.path_tar, "r:gz")
        for count, member in enumerate(tar.getmembers()):
            if os.path.splitext(member.name)[1] == ".json":
                print(f"Processing {count}-th file {member.name}")
                f = tar.extractfile(member)
                content = f.read()
                js = json.loads(content)
                playlist_df, playlist_track_df, track_df = process_json(js)
                playlist_df.to_sql('playlist', con=self.engine, if_exists='append', index=False)
                playlist_track_df.to_sql('playlist_track', con=self.engine, if_exists='append', index=False)
                track_df.to_sql('track', con=self.engine, if_exists='append', index=False)
                con.execute("""DELETE FROM track T1 USING track T2 
                                WHERE   T1.ctid < T2.ctid AND T1.track_uri  = T2.track_uri;""")

    def update_db(self):
        con = self.engine.connect()
        if self.columns_exists(table_name='playlist', column_name='id'):
            return
        con.execute("""ALTER TABLE playlist ADD COLUMN id SERIAL PRIMARY KEY;""")
        con.execute("""ALTER TABLE track ADD COLUMN id SERIAL PRIMARY KEY;""")
        con.execute("""CREATE TABLE playlist_track_temp AS 
                    SELECT playlist.id as playlist_id, track.id as track_id, playlist_track.pos 
                    FROM playlist_track  
                    JOIN  playlist ON playlist_track.pid = playlist.pid 
                    JOIN track ON playlist_track.tracks = track.track_uri;""")
        con.execute("""DROP TABLE playlist_track;""")
        con.execute("""ALTER TABLE playlist_track_temp RENAME TO playlist_track;""")
        con.execute("""ALTER TABLE playlist_track 
                              ADD CONSTRAINT fk_track_id FOREIGN KEY (track_id) REFERENCES track (id);""")

    def download_audio_features(self, step=100):
        if self.columns_exists(table_name='track', column_name='energy'):
            return
        df_track = pd.read_sql_table('track', con=self.engine)
        audio_features = pd.DataFrame()
        audio_cols = ["danceability", "energy", "key", "loudness", "mode", "speechiness", "acousticness",
                      "instrumentalness", "liveness", "valence", "tempo", "uri"]
        for i in range(0, len(df_track), step):
            if i % 10000 == 0:
                print(f'{i}-TH TRACK IS PROCESSING...')
            for attempt in range(5):
                try:
                    df_slice = df_track['track_uri'][i:i + step]
                    temp_audio_features = pd.DataFrame([x for x in self.sp.audio_features(df_slice)
                                                        if x is not None])[audio_cols]
                    audio_features = pd.concat([audio_features, temp_audio_features])
                except AttributeError as e:
                    logging.exception(f"{e} occurred FROM {i} TO {i + step}")
                    break
                except requests.exceptions.ReadTimeout or requests.exceptions.ConnectionError as e:
                    logging.exception(f"{e} occurred from {i} to {i + step} step")
                    time.sleep(10)
                    continue
                except ValueError as e:
                    logging.exception(f"{e} occurred from {i} to {i + step} step")
                    continue
                else:
                    break
        audio_features.to_sql('audio_features', if_exists='replace', index=False, con=self.engine)
        con = self.engine.connect()
        con.execute("""CREATE TABLE IF NOT EXISTS track_temp AS 
                       SELECT *  FROM  track LEFT JOIN  audio_features ON  audio_features.uri = track.track_uri; """)
        con.execute("""DROP TABLE track;""")
        con.execute("""ALTER TABLE track_temp RENAME TO track;""")
        con.execute("""ALTER TABLE track ADD CONSTRAINT id_pk PRIMARY KEY (id);""")
        con.execute("""ALTER TABLE playlist_track 
                                     ADD CONSTRAINT fk_track_id FOREIGN KEY (track_id) REFERENCES track (id);""")
        con.execute("""DROP TABLE audio_features;""")
        con.execute("""ALTER TABLE track DROP COLUMN track_uri;""")



    def load_random(self, num_playlists):
        playlist_track_random = pd.read_sql_query(f'WITH random_pid AS '
                                                  f'(select pid from playlist order by random() limit {num_playlists}) '
                                                  f'select playlist_track.pid, track_uri from playlist_track,random_pid'
                                                  f' WHERE playlist_track.pid = random_pid.pid', con=self.engine)
        self.download_songs(playlist_track_random['track_uri'])

    def download_songs(self, series_uri):
        if not os.path.exists(self.song_directory):
            os.makedirs(self.song_directory)
        songs_exists = series_uri.apply(lambda x: os.path.exists(os.path.join(self.song_directory, f'{x}')))
        series_uri_new = series_uri[~songs_exists]
        print("{} SONGS ARE TO BE DOWNLOADED".format(len(series_uri_new)))

    def download_song(self, num, spotify_track_uri):
        print("DOWNLOADING SONG № {}".format(num))
        track_url = self.sp.track(spotify_track_uri)['external_urls']['spotify']
        saved_directory = self.download_song_from_youtube(track_url)
        new_path = self.extract_song_from_folder(saved_directory, rename=spotify_track_uri)
        return new_path

    def download_song_from_youtube(self, track_url):
        os.putenv("SPOTIPY_CLIENT_ID", my_config['SPOTIFY']['CLIENT_ID'])
        os.putenv("SPOTIPY_CLIENT_SECRET", my_config['SPOTIFY']['CLIENT_SECRET'])
        os.putenv("SPOTIPY_REDIRECT_URI", my_config['SPOTIFY']['REDIRECT_URI'])
        bash_command = "spotify_dl -l {} -s -o {}".format(track_url, self.song_directory)
        output = ""
        try:
            process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
            output, error = process.communicate()
            saved_directory = re.findall('Saving songs to ([^"]*) directory', output.decode("utf-8"))[0]
        except AttributeError:
            print("You need to install spotify_dl")
            raise
        except IndexError:
            print(output)
            print("You need to set your Spotify API credentials")
            raise
        saved_directory = saved_directory.replace("\n", '')
        return saved_directory
