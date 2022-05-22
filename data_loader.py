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


class DataLoader:
    def __init__(self):
        self.root_dir = my_config['SPOTIFY']['DATA_PATH']
        self.jsons_dir = os.path.join(self.root_dir, "data/")
        self.song_directory = os.path.join(self.root_dir, "songs/")

        self.engine = create_engine(f"postgresql+psycopg2://{my_config['DATABASE']['USERNAME']}:"
                                    f"{my_config['DATABASE']['PASSWORD']}@{my_config['DATABASE']['HOST']}:"
                                    f"{my_config['DATABASE']['PORT']}/{my_config['DATABASE']['DB']}")
        self.sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=my_config['SPOTIFY']['CLIENT_ID'],
                                                            client_secret=my_config['SPOTIFY']['CLIENT_SECRET'],
                                                            redirect_uri=my_config['SPOTIFY']['REDIRECT_URI'],
                                                            scope="user-library-read"))

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

    def extract_song_from_folder(self, saved_directory, rename):
        dir_path = self.song_directory + saved_directory
        new_path = None
        for root, dirs, files in os.walk(dir_path):
            for file in files:
                if file.endswith('.mp3'):
                    new_path = self.song_directory + rename
                    os.rename(dir_path + "/" + file, new_path)
        shutil.rmtree(dir_path)
        return new_path

    def get_audio_features(self, track_path, step):
        df_track = pd.read_csv(f"{self.jsons_dir}/{track_path}")
        audio_features = pd.DataFrame()

        for i in range(0, len(df_track), step):
            if i % 100000 == 0:
                print(f'{i}-TH TRACK IS PROCESSING...')
            for attempt in range(2):
                try:
                    audio_features = pd.concat([audio_features,
                                                pd.DataFrame(
                                                    self.sp.audio_features(df_track['track_uri'][i:i + step]))])
                except AttributeError:
                    print(f"    AttributeError OCCURRED FROM {i} TO {i + step}")
                    break
                except requests.exceptions.ReadTimeout:
                    print(f"    ReadTimeout ERROR OCCURRED FROM {i} TO {i + step}")
                    print(f"        {attempt}-TH RETRY")
                    time.sleep(10)
                    continue
                else:
                    break
        audio_features.drop(['duration_ms'], axis=1, inplace=True)
        return df_track.set_index('track_uri').join(audio_features.set_index('uri'), how='left')

    def load_audio_features_to_db(self):
        if self.table_exists('track'):
            return 1
        audio_features = self.get_audio_features(track_path='track_full.csv', step=100)
        audio_features.to_sql('track', con=self.engine, if_exists='replace', index=False, chunksize=5)

    def table_exists(self, name):
        ins = inspect(self.engine)
        ret = ins.dialect.has_table(self.engine.connect(), name)
        print('Table "{}" exists: {}'.format(name, ret))
        return ret

    def load_csv_tables_to_db(self):
        for filename in ['playlist_full.csv', 'playlist_track_full.csv']:
            if not self.table_exists(filename[:-9]):
                df = pd.read_csv(f"{self.jsons_dir}{filename}")
                df.to_sql(filename[:-9], con=self.engine, if_exists='replace', index=False, chunksize=10)

    def create_playlist_tracks(self):
        return pd.read_sql_query(
            f"""SELECT playlist_primary_id, array_agg(track_primary_id) AS tracks_in_playlist FROM playlist_track_int 
            GROUP BY playlist_primary_id ORDER BY RANDOM()""", con=self.engine)

    def load_playlist_tracks_to_db(self):
        self.create_playlist_tracks().to_sql(name='playlist_tracks', con=self.engine, if_exists='replace', index=False)
