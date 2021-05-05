from text_search_over_documets_abc_meta import TextSearchDocumentABCMeta
from elasticsearch import Elasticsearch
from enum import Enum
import math


class TextSearchDocuments(TextSearchDocumentABCMeta):

    # TODO
    class _Index(Enum):
        pass

    def __init__(self, connect_url):
        self.es = Elasticsearch(connect_url)

    def _make_query(self, string_look_for) -> dict:
        query_body = {
            "query": {
                "wildcard": {
                    'text': f'*{string_look_for}*'
                }
            }
        }

        return query_body

    def _get_index_from_object_type(self, object_type) -> str:
        # TODO change to enum name ?
        return f"{object_type}_index"

    def _get_keys_from_search_results(self, search_result_list) -> list:
        keys_result_list = []
        for search_result in search_result_list['hits']['hits']:
            keys_result_list.append(search_result['_source']['key'])
        return keys_result_list

    def check_for_text(self, object_type, string_look_for, max_list_size) -> list:
        query = self._make_query(string_look_for=string_look_for)
        search_index = self._get_index_from_object_type(object_type)
        search_result_list = self.es.search(index=search_index, body=query, size=max_list_size)
        '''search_result = self.es.get(index=search_index, id=0)['_source']
        print(search_result)
        search_result = self.es.get(index=search_index, id=1)['_source']
        print(search_result)'''
        keys_result_list = self._get_keys_from_search_results(search_result_list=search_result_list)
        return keys_result_list
