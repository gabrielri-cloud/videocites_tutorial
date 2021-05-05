import abc


class TextSearchDocumentABCMeta(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def check_for_text(self, object_type, string_look_for) -> list:
        raise NotImplementedError
