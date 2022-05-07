from sqlalchemy import create_engine
import pandas as pd

from get_songs import download_songs
from myconstants import *
from load_data_from_spotify_api import get_audio_features
from convert_data import make_csv_files_from_json_files



def main():
    engine = create_engine(f"postgresql+psycopg2://{CREDENTIALS['DATABASE']['USERNAME']}:"
                           f"{CREDENTIALS['DATABASE']['PASSWORD']}@{CREDENTIALS['DATABASE']['HOST']}:"
                           f"{CREDENTIALS['DATABASE']['PORT']}/{CREDENTIALS['DATABASE']['DB']}")

    # print(pd.read_sql_query('WITH random_pid AS '
    #                                               '(select pid from playlist order by random() limit 10) '
    #                                               'select playlist_track.pid, track_uri from playlist_track, random_pid'
    #                                               ' WHERE playlist_track.pid = random_pid.pid', con=engine))
    if DATA_IS_NOT_CONVERTED:
        make_csv_files_from_json_files()
    if AUDIO_FEATURES_ARE_NOT_PARSED:
        audio_features = get_audio_features(track_path='track_full.csv', step=100)
        audio_features.to_sql('track', con=engine, if_exists='replace', index=False, chunksize=5)
    if TABLES_ARE_NOT_CREATED:
        for filename in ['playlist_full.csv', 'playlist_track_full.csv']:
            df = pd.read_csv(f"{SPOTIFY_DATA_PATH}/{filename}")
            df.to_sql(filename[:-9], con=engine, if_exists='replace', index=False, chunksize=10)
    if SONGS_ARE_NOT_DOWNLOADED:
        playlist_track_random = pd.read_sql_query('WITH random_pid AS '
                                                  '(select pid from playlist order by random() limit 100) '
                                                  'select playlist_track.pid, track_uri from playlist_track, random_pid'
                                                  ' WHERE playlist_track.pid = random_pid.pid', con=engine)
        playlist_track_random['track_path'] = download_songs(playlist_track_random['track_uri'])
        playlist_track_random.to_pickle(SPOTIFY_DATA_PATH + "/dataframes/playlist_track_random.pkl")
    return 0


if __name__ == '__main__':
    main()
