import pandas as pd

from modelling.similarity_model import SimilarityModel
from modelling.base_model import BaseModel
from modelling.random_model import RandomModel
from modelling.annoy_model import AnnoyModel


class Train:
    def __init__(self, model_name):
        self.models = {"cos_similarity": SimilarityModel, "random": RandomModel, "annoy": AnnoyModel}
        self.model = self.models[model_name]

    def get_model(self, data) -> BaseModel:
        return self.model(data=data)

    def train(self):
        data = pd.read_pickle("data.pkl")
        model = self.get_model(data=data)
        model.train()
