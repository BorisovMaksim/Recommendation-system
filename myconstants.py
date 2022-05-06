import yaml

CREDENTIALS = yaml.safe_load(open('/home/maksim/Data/credentials.yml'))
SPOTIFY_DATA_PATH = '/home/maksim/Data/Spotify'
SPOTIFY_JSONS_PATH = '/home/maksim/Data/Spotify/data'

DATA_IS_NOT_CONVERTED = False
AUDIO_FEATURES_ARE_NOT_PARSED = False
TABLES_ARE_NOT_CREATED = False
SONGS_ARE_NOT_DOWNLOADED = False
