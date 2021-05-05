import abc


class TextSearchDocumentABCMeta(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def check_for_text(self, object_type, string_look_for, max_list_size) -> list:
        raise NotImplementedError
