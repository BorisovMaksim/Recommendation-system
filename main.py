from app import App


def main(stage, model_name):
    system = App(stage=stage, model_name=model_name)
    system.collect_data()
    system.extract_features()

    # system.process_raw_data()
    # system.load_data_to_db()
    # system.download_songs()
    # system.train()
    return 1


if __name__ == '__main__':
    main("train", "annoy")
