from app import App


def main(ml_stage):
    system = App(stage=ml_stage, model_id="my_model_name")
    system.process_raw_data()
    system.load_data_to_db()
    system.download_songs()
    system.train()
    # df = system.load_premodel_data()

    # train, test = system.train_test_split(df, split_size=0.8)
    # system.fit(train)

    return 1


if __name__ == '__main__':
    main("")
