import abc


class BaseStore(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def count_data(self, data_type=None, *args, **kwargs):
        return NotImplementedError

    @abc.abstractmethod
    def list_data(self, data_type=None, *args, **kwargs):
        return NotImplementedError

    @abc.abstractmethod
    def check_data(self, position, data_type=None, *args, **kwargs):
        return NotImplementedError

    @abc.abstractmethod
    def get_data(self, position, data_type=None, *args, **kwargs):
        return NotImplementedError

    @abc.abstractmethod
    def save_data(self, position, data, data_type=None, *args, **kwargs):
        return NotImplementedError

    @abc.abstractmethod
    def delete_data(self, position, data_type=None, *args, **kwargs):
        return NotImplementedError

    @abc.abstractmethod
    def get_position(self, position, data_type=None, *args, **kwargs):
        return NotImplementedError

    @abc.abstractmethod
    def get_data_size(self, position, data_type=None, *args, **kwargs):
        return NotImplementedError

    @abc.abstractmethod
    def check_self(self, *args, **kwargs):
        return NotImplementedError

    @abc.abstractmethod
    def save_self(self, *args, **kwargs):
        return NotImplementedError

    @abc.abstractmethod
    def free_self(self, *args, **kwargs):
        return NotImplementedError
