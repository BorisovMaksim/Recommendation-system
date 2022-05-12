from app import App


def main(ml_stage):
    model_id = "my_model_name"
    m1 = App(model_id, ml_stage)
    m1.create_premodel_data()
    df = m1.load_premodel_data()
    if ml_stage == "train":
        train, test = m1.train_test_split(df, split_size=0.8)
    else:
        test = df
    del df

    return 1


if __name__ == '__main__':
    main("")
