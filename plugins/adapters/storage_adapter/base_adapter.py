# airflow DAG
from abc import ABC, abstractmethod
from typing import Iterator, List


class BaseStorageAdapter(ABC):
    @abstractmethod
    def list_files(self, path):
        pass

    @abstractmethod
    def read_file_chunks(self, file_path, chunk_size):
        pass

    @abstractmethod
    def write_file_chunks(self, file_path, chunks):
        pass

    @abstractmethod
    def delete_file(self, file_path):
        pass

    @abstractmethod
    def rename_file(self, old_path, new_path):
        pass