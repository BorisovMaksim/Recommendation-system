import os
import shutil
from config import my_config


class DataCleaner:
    def __init__(self):
        self.root_dir = my_config['SPOTIFY']['DATA_PATH']

    def clean(self, retain="songs"):
        os.chdir(self.root_dir)
        for item in os.listdir(os.getcwd()):
            if item not in retain:
                self.remove(item)

    def remove(self, path):
        if os.path.isdir(path):
            try:
                shutil.rmtree(path)
            except OSError:
                print(f"Unable to remove folder: {path}")
        else:
            try:
                if os.path.exists(path):
                    os.remove(path)
            except OSError:
                print(f"Unable to remove file: {path}")
