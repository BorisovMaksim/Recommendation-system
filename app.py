from preprocessing.data_loader import DataLoader
from training import Train


class App:
    def __init__(self, model_name):
        if model_name not in ['random', 'cos_similarity', 'annoy']:
            raise ValueError("Models are: \n1.random\n2. cos_similarity\n3. annoy")
        self.trainer = Train(model_name=model_name)
        self.loader = DataLoader()

    def collect_data(self):
        self.loader.load_data_to_db()
        self.loader.update_db()
        self.loader.download_audio_features()

    def extract_features(self):
        self.loader.aggregate_playlists()

    def download_songs(self, num_playlists=10):
        self.loader.load_random(num_playlists=num_playlists)

    def train(self):
        self.trainer.train()
