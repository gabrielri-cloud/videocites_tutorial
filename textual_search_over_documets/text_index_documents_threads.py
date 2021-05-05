from text_index_documents_abc_meta import TextIndexDocumentsABCMeta
from elasticsearch import Elasticsearch
from enum import Enum

from threading import Thread



class TextIndexDocuments(TextIndexDocumentsABCMeta):
    # TODO
    class _Index(Enum):
        pass

    def __init__(self, connect_url):
        self.es = Elasticsearch(connect_url)

    def _get_text_from_filed(self, document, field):
        # TODO check if fields exist, throw exception? or do nothing and continue with the next
        split_by_point_list = field.split(".")
        tmp_json_or_text = document.get(split_by_point_list[0])
        if len(split_by_point_list) > 1:
            for j in range(1, len(split_by_point_list)):
                tmp_json_or_text = tmp_json_or_text.get(split_by_point_list[j])
        return tmp_json_or_text

    def _get_text_from_document_fields(self, document, search_field_list):
        praised_text = ""
        for field in search_field_list:
            # TODO decide split char
            praised_text += " " + self._get_text_from_filed(document=document, field=field)
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
        res = self.es.index(index=index_in, id=tmp_i, body=document_to_add)
        if res['result'] != 'updated':
            raise RuntimeError('document was not updated to elasticsearch')

    def _add_document(self, document, search_field_list, key, object_type, i):
        text_to_current_doc = self._get_text_from_document_fields(document=document,
                                                                  search_field_list=search_field_list)
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
        self.es.indices.refresh()

