from data_converter import DataConverter
from data_loader import DataLoader


def main():
    converter = DataConverter()
    converter.make_csv_files_from_json_files()
    loader = DataLoader()
    loader.load_audio_features_to_db()
    loader.load_csv_tables_to_db()
    loader.load_random(num_playlists=3)
    return 1


if __name__ == '__main__':
    main()
