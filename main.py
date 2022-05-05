import convert_data
import load_data_to_database
from sqlalchemy import create_engine
import pandas as pd
from get_songs import download_songs
from myconstants import *





def main():
    engine = create_engine('postgresql+psycopg2://maksim:{}@localhost/spotify'
                           .format(CREDENTIALS['maksim']['password']))
    if DATA_IS_NOT_CONVERTED:
        convert_data.make_csv_files_from_json_files()
    if TABLES_ARE_NOT_CREATED:
        for filename in ['playlist_full.csv', 'track_full.csv', 'playlist_track_full.csv']:
            load_data_to_database.csv_table_to_sql(engine, SPOTIFY_DATA_PATH, filename)
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
