from modelling.similarity_model import SimilarityModel
from modelling.base_model import BaseModel
from modelling.random_model import RandomModel
from modelling.annoy_model import AnnoyModel


class Train:
    def __init__(self, model_name):
        self.models = {"cos_similarity": SimilarityModel, "random": RandomModel, "annoy": AnnoyModel}
        self.model = self.models[model_name]

    def get_model(self, track, train, test) -> BaseModel:
        return self.model(track=track, playlist_train=train, playlist_test=test)

    def train(self):
        model = self.get_model()
        pass
