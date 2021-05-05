from text_index_documents_abc_meta import TextIndexDocumentsABCMeta
from elasticsearch import Elasticsearch
from enum import Enum


@TextIndexDocumentsABCMeta.register
class TextIndexDocuments:
    # TODO
    class Index(Enum):
        pass

    def __init__(self, connect_url):
        self.es = Elasticsearch(connect_url)

    def __get_text_from_filed(self, document, field):
        # TODO check if fields exist, throw exception? or do nothing and continue with the next
        split_by_point_list = field.split(".")
        tmp_json_or_text = document.get(split_by_point_list[0])
        if len(split_by_point_list) > 1:
            for j in range(1, len(split_by_point_list)):
                tmp_json_or_text = tmp_json_or_text.get(split_by_point_list[j])
        return tmp_json_or_text

    def __get_text_from_document_fields(self, document, search_field_list):
        praised_text = ""
        for field in search_field_list:
            # TODO decide split char
            praised_text += " " + self.__get_text_from_filed(document=document, field=field)
        return praised_text

    def __create_search_document(self, document, text, key):
        search_document = {
            'key': document.get(key),
            'text': text
        }
        return search_document

    def __insert_document(self, object_type, document_to_add, tmp_i):
        # TODO check if needed id. same document shouldn't be added twice? maybe got changed? we want to remove old
        #  documents?
        # TODO change to enum
        index_in = f"{object_type}_index"
        res = self.es.index(index=index_in, id=tmp_i, body=document_to_add)
        if res['result'] != 'updated':
            # TODO decide what to do if not updated
            print('Error - not updated')

    def add_documents(self, docs_to_check_list, search_field_list, key, object_type):
        for i in range(0, len(docs_to_check_list)):
            text_to_currect_doc = self.__get_text_from_document_fields(document=docs_to_check_list[i],
                                                                       search_field_list=search_field_list)
            document_to_add = self.__create_search_document(document=docs_to_check_list[i], text=text_to_currect_doc,
                                                            key=key)
            self.__insert_document(object_type=object_type, document_to_add=document_to_add,
                                   tmp_i=i)  # TODO change tmp_i
