import convert_data
import load_data_to_database
from sqlalchemy import create_engine
import psycopg2
import yaml


DATA_IS_NOT_CONVERTED = False
TABLES_ARE_NOT_CREATED = False
CREDENTIALS = yaml.safe_load(open('/home/maksim/Documents/credentials.yml'))
PATH = '/home/maksim/Documents/ML/Datasets/Spotify_data/'


def main():
    if DATA_IS_NOT_CONVERTED:
        convert_data.make_csv_files_from_json_files()
    if TABLES_ARE_NOT_CREATED:
        engine = create_engine('postgresql+psycopg2://maksim:{}@localhost/spotify'
                               .format(CREDENTIALS['maksim']['password']))
        for filename in ['playlist_full.csv', 'track_full.csv', 'playlist_track_full.csv']:
            load_data_to_database.csv_table_to_sql(engine, PATH, filename)

    return 0


if __name__ == '__main__':
    main()
