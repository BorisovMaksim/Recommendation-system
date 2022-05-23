from app import App


def main(ml_stage):
    system = App(stage=ml_stage, model_id="similarity")
    system.process_raw_data()
    system.load_data_to_db()
    system.download_songs()
    system.train()
    system.test()
    return 1


if __name__ == '__main__':
    main("test")
