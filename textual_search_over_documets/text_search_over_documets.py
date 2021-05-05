from text_search_over_documets_abc_meta import TextSearchDocumentABCMeta
from elasticsearch import Elasticsearch
from enum import Enum


@TextSearchDocumentABCMeta.register
class TextSearchDocuments:

    # TODO
    class Index(Enum):
        pass

    def __init__(self, connect_url):
        self.es = Elasticsearch(connect_url)

    def __make_query(self, string_look_for) -> dict:
        query_body = {
            "query": {
                "match": {
                    'text': f'{string_look_for}'
                }
            }
        }
        return query_body

    def __get_index_from_object_type(self, object_type):
        # TODO change to enum name ?
        return f"{object_type}_index"

    def __get_keys_from_search_results(self, search_result_list):
        keys_result_list = []
        for search_result in search_result_list['hits']['hits']:
            keys_result_list.append(search_result['_source']['key'])
        return keys_result_list

    def check_for_text(self, object_type, string_look_for) -> list:
        query = self.__make_query(string_look_for=string_look_for)
        search_index = self.__get_index_from_object_type(object_type)
        search_result_list = self.es.search(index=search_index, body=query)
        '''search_result = self.es.get(index=search_index, id=0)['_source']
        print(search_result)
        search_result = self.es.get(index=search_index, id=1)['_source']
        print(search_result)'''
        keys_result_list = self.__get_keys_from_search_results(search_result_list=search_result_list)
        return keys_result_list
