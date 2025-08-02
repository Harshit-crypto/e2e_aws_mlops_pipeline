import os
import urllib.request as request
from src.datascience_project import logger
import zipfile
from src.datascience_project.entity.config_entity import DataIngestionConfig



class DataIngestion:
    def __init__(self, config: DataIngestionConfig):
        self.config = config

    def download_file(self):
        if not os.path.exists(self.config.local_data_file):
            
            filename, headers = request.urlretrieve(self.config.source_URL, self.config.local_data_file)
            logger.info(f"{filename} downloaded! info: \n{headers}")
        else:
            logger.info(f"File {self.config.local_data_file} already exists. Skipping download.")

    def extract_zip_file(self):
        unzip_path = self.config.unzip_dir
        if not os.path.exists(unzip_path):
            os.makedirs(unzip_path)
            #logger.info(f"Created directory: {unzip_path}")
        with zipfile.ZipFile(self.config.local_data_file, 'r') as zip_ref:
            zip_ref.extractall(unzip_path)
            logger.info(f"Extracted files to {unzip_path}")