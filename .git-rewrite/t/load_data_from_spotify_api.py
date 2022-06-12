import spotipy
from spotipy.oauth2 import SpotifyOAuth
from myconstants import CREDENTIALS
import pandas as pd
from myconstants import SPOTIFY_DATA_PATH


def get_audio_features(track_path, step):
    df_track = pd.read_csv(f"{SPOTIFY_DATA_PATH}/{track_path}")
    auth_manager = SpotifyOAuth(client_id=CREDENTIALS['maksim']['SPOTIPY_CLIENT_ID'],
                                client_secret=CREDENTIALS['maksim']['SPOTIPY_CLIENT_SECRET'],
                                redirect_uri=CREDENTIALS['maksim']['SPOTIPY_REDIRECT_URI'],
                                scope="user-library-read")
    sp = spotipy.Spotify(auth_manager=auth_manager)
    audio_features = pd.DataFrame()
    for i in range(0, len(df_track), step):
        audio_features = pd.concat([audio_features, pd.DataFrame(sp.audio_features(df_track['track_uri'][i:i+50]))])
    audio_features.drop(['duration_ms'], axis=1, inplace=True)
    return df_track.set_index('track_uri').join(audio_features.set_index('uri'), how='left')


