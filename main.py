from app import App


def main(model_name):
    system = App(model_name=model_name)
    system.collect_data()
    system.extract_features()
    system.train()
    return 1


if __name__ == '__main__':
    main("annoy")
