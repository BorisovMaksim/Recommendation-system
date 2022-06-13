import pandas as pd
from data_loader import DataLoader
from modelling.similarity_model import SimilarityModel
from modelling.random_model import RandomModel
from modelling.annoy_model import AnnoyModel


class App:
    def __init__(self, model_name):
        if model_name not in ['random', 'cos_similarity', 'annoy']:
            raise ValueError("Models are: \n1.random\n2. cos_similarity\n3. annoy")
        self.loader = DataLoader()
        self.models = {"cos_similarity": SimilarityModel, "random": RandomModel, "annoy": AnnoyModel}
        self.model = self.models[model_name]

    def collect_data(self):
        self.loader.load_data_to_db()
        self.loader.update_db()
        self.loader.download_audio_features()

    def extract_features(self):
        self.loader.aggregate_playlists()

    def train(self):
        data = pd.read_pickle("data.pkl").set_index('id')
        model = self.model(engine=self.loader.engine, data=data)
        model.process_data()
        model.train()
        self.model = model

    def download_songs(self, num_playlists=10):
        self.loader.load_random(num_playlists=num_playlists)