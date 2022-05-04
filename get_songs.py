import subprocess
import re
import spotipy
import os
from spotipy.oauth2 import SpotifyOAuth
import shutil
from myconstants import CREDENTIALS, SONG_PATH
import pandas as pd


def download_songs(series_uri):
    print("{} SONGS ARE TO BE DOWNLOADED".format(len(series_uri)))
    return pd.Series(download_song(x[0], x[1]) for x in enumerate(series_uri))


def download_song(num, spotify_track_uri):
    print("DOWNLOADING SONG № {}".format(num))
    track_url = get_track_url(spotify_track_uri)
    saved_directory = download_song_from_youtube(track_url)
    new_path = extract_song_from_folder(saved_directory, rename=spotify_track_uri)
    return new_path


def get_track_url(track_uri):
    auth_manager = SpotifyOAuth(client_id=CREDENTIALS['maksim']['SPOTIPY_CLIENT_ID'],
                                client_secret=CREDENTIALS['maksim']['SPOTIPY_CLIENT_SECRET'],
                                redirect_uri=CREDENTIALS['maksim']['SPOTIPY_REDIRECT_URI'],
                                scope="user-library-read")
    sp = spotipy.Spotify(auth_manager=auth_manager)
    track_url = sp.track(track_uri)['external_urls']['spotify']
    return track_url


def download_song_from_youtube(track_url):
    bash_command = "spotify_dl -l {} -s -o {}".format(track_url, SONG_PATH)
    error, output = None, None
    try:
        process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
    except Exception:
        print("spotify_dl exception {}".format(error))
    saved_directory = re.findall('Saving songs to ([^"]*) directory', output.decode("utf-8"))[0]
    return saved_directory


def extract_song_from_folder(saved_directory, rename):
    dir_path = SONG_PATH + saved_directory
    new_path = None
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            if file.endswith('.mp3'):
                new_path = SONG_PATH + rename
                os.rename(dir_path + "/" + file, new_path)
    shutil.rmtree(dir_path)
    return new_path
