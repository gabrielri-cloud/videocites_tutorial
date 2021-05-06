from text_index_documents_abc_meta import TextIndexDocumentsABCMeta
from elasticsearch import Elasticsearch
from enum import Enum

from threading import Thread


class TextIndexDocuments(TextIndexDocumentsABCMeta):
    # TODO
    class Index(Enum):
        pass

    def __init__(self, connect_url):
        self._es = Elasticsearch(connect_url)

    def _get_text_from_field(self, json_or_list, split_by_point_list, separate_string, place):
        if place >= len(split_by_point_list):
            return json_or_list
        praised_text_field = ""
        if isinstance(json_or_list, list):
            for member in json_or_list:
                praised_text_field += separate_string + self._get_text_from_field(json_or_list=member,
                                                                                  split_by_point_list=split_by_point_list,
                                                                                  separate_string=separate_string,
                                                                                  place=place)
        else:
            praised_text_field = self._get_text_from_field(json_or_list=json_or_list.get(split_by_point_list[place]),
                                                           split_by_point_list=split_by_point_list,
                                                           separate_string=separate_string,
                                                           place=place + 1)

        return praised_text_field

    def _get_text_from_document_fields(self, document, search_field_list, separate_string=" "):
        praised_text = ""
        for field in search_field_list:
            # TODO decide split char
            split_by_point_list = field.split(".")
            praised_text += separate_string + self._get_text_from_field(
                json_or_list=document.get(split_by_point_list[0]),
                split_by_point_list=split_by_point_list,
                separate_string=separate_string,
                place=1)
        return praised_text

    def _create_search_document(self, document, text, key):
        search_document = {
            'key': document.get(key),
            'text': text
        }
        return search_document

    def _insert_document(self, object_type, document_to_add, tmp_i):
        # TODO check if needed id. same document shouldn't be added twice? maybe got changed? we want to remove old
        #  documents?
        # TODO change to enum
        index_in = f"{object_type}_index"
        res = self._es.index(index=index_in, id=tmp_i, body=document_to_add)
        if res['result'] != 'updated' and res['result'] != 'created':
            raise RuntimeError('document was not updated to elasticsearch')

    def _add_document(self, document, search_field_list, key, object_type, i):
        try:
            text_to_current_doc = self._get_text_from_document_fields(document=document,
                                                                      search_field_list=search_field_list)
        except TypeError:
            raise RuntimeError('field not exist in document')

        document_to_add = self._create_search_document(document=document, text=text_to_current_doc,
                                                       key=key)
        self._insert_document(object_type=object_type, document_to_add=document_to_add,
                              tmp_i=i)  # TODO change tmp_i, need to know something unique so it wont be overwritten

    def add_documents(self, docs_to_check_list, search_field_list, key, object_type):
        thread_list = []
        for i, document in enumerate(docs_to_check_list):
            thread = Thread(target=self._add_document, args=(document, search_field_list, key, object_type, i))
            thread_list.append(thread)
            thread.start()
        for thread in thread_list:
            thread.join()
        self._es.indices.refresh()

    def disconnect(self):
        self._es.transport.close()
