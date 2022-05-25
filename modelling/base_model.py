from abc import ABC, abstractmethod


class BaseModel(ABC):
    @abstractmethod
    def train(self):
        pass

    @abstractmethod
    def test(self):
        pass

    @abstractmethod
    def process_data(self):
        pass
