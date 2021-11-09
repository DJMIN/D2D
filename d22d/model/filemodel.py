import os

from d22d.model import midhardware


class FileSystemStore(midhardware.BaseStore):
    def __init__(self, location):
        self.location = location

    def count_data(self):
        return NotImplementedError

    def list_data(self):
        return NotImplementedError

    def get_data(self, position, *args, **kwargs):
        return NotImplementedError

    def save_data(self, position, *args, **kwargs):
        return NotImplementedError

    def delete_data(self, position, *args, **kwargs):
        return NotImplementedError

    def get_position(self, position, *args, **kwargs):
        return os.path.join(self.location, position)

    def get_data_size(self, position, *args, **kwargs):
        return NotImplementedError
