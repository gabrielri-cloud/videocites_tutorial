import json
import unittest
from datetime import datetime
from time import time
from elasticsearch import Elasticsearch
import collections

from text_index_documents_threads import TextIndexDocuments
from text_search_over_documets import TextSearchDocuments


class MyTestCase(unittest.TestCase):

    def setUp(self) -> None:
        pass


    def test_indexing_and_search_small_amount(self):
        amount_in = 10
        list_docs = []
        for i in range(0, amount_in):
            list_docs.append({
                'id': i,
                'author': 'kimchy',
                'title1': 'heeey: gogigaa. bonsai cool.',
                'title2': 'Elasticsearch: cool. bonsai c.',
                'timestamp': datetime.now(),
                'nested1': {
                    'nested2': 'eaeeea',
                },
                'desc': 'blabla'
            })
        input_key = 'id'
        input_look_for = 'eaeeea'
        input_list_fields = ['title1', 'title2', 'nested1.nested2']
        input_object_type = "object_type_name"
        txt_index_documents_val = TextIndexDocuments(connect_url='http://localhost:9200/')
        txt_search_documents_val = TextSearchDocuments(connect_url='http://localhost:9200/')
        index_start_time = time()
        txt_index_documents_val.add_documents(docs_to_check_list=list_docs,
                                              search_field_list=input_list_fields,
                                              key=input_key,
                                              object_type=input_object_type)
        print(f"indexing time {amount_in}: {time() - index_start_time}")
        search_start_time = time()
        res = txt_search_documents_val.check_for_text(object_type=input_object_type,
                                                      string_look_for=input_look_for, max_list_size=1000)
        print(f"searching time {amount_in}: {time() - search_start_time}")
        self.assertTrue(collections.Counter(res) == collections.Counter([i for i in range(0, amount_in)]))
        txt_index_documents_val.disconnect()
        txt_search_documents_val.disconnect()

        input_object_type = "object_type_name"
        es = Elasticsearch(connect_url='http://localhost:9200/')
        index_in = f"{input_object_type}_index"
        for i in range(0, amount_in):
            es.delete(index=index_in, id=i)
        es.transport.close()

    def test_indexing_and_search_big_amount(self):
        amount_in = 1000
        list_docs = []
        for i in range(0, amount_in):
            list_docs.append({
                'id': i,
                'author': 'kimchy',
                'title1': 'heeey: gogigaa. bonsai cool.',
                'title2': 'Elasticsearch: cool. bonsai c.',
                'timestamp': datetime.now(),
                'nested1': {
                    'nested2': 'eaeeea',
                },
                'desc': 'blabla'
            })
        input_key = 'id'
        input_look_for = 'eaeeea'
        input_list_fields = ['title1', 'title2', 'nested1.nested2']
        input_object_type = "object_type_name"
        txt_index_documents_val = TextIndexDocuments(connect_url='http://localhost:9200/')
        txt_search_documents_val = TextSearchDocuments(connect_url='http://localhost:9200/')
        index_start_time = time()
        txt_index_documents_val.add_documents(docs_to_check_list=list_docs,
                                              search_field_list=input_list_fields,
                                              key=input_key,
                                              object_type=input_object_type)
        print(f"indexing time {amount_in}: {time() - index_start_time}")
        search_start_time = time()
        res = txt_search_documents_val.check_for_text(object_type=input_object_type,
                                                      string_look_for=input_look_for, max_list_size=1000)
        print(f"searching time {amount_in}: {time() - search_start_time}")
        self.assertTrue(collections.Counter(res) == collections.Counter([i for i in range(0, amount_in)]))
        txt_index_documents_val.disconnect()
        txt_search_documents_val.disconnect()

        input_object_type = "object_type_name"
        es = Elasticsearch(connect_url='http://localhost:9200/')
        index_in = f"{input_object_type}_index"
        for i in range(0, amount_in):
            es.delete(index=index_in, id=i)
        es.transport.close()

    def test_empty(self):
        input_object_type = "object_type_name"
        es = Elasticsearch(connect_url='http://localhost:9200/')
        index_in = f"{input_object_type}_index"
        query_body = {
            "query": {
                "match_all": {}
            }
        }
        search_result_list = es.search(index=index_in, body=query_body, size=1000)
        self.assertEqual(search_result_list['hits']['total']['value'], 0)
        es.transport.close()

    def test_real_json(self):
        with open('data.json') as f:
            data = json.load(f)
            list_docs = data['data']['entities']['items']
            input_key = 'id'
            input_look_for = 'NBA'
            input_list_fields = ['description', 'keywords.text', 'keywords.tags.display_name']
            input_object_type = "items"
            txt_index_documents_val = TextIndexDocuments(connect_url='http://localhost:9200/')
            txt_search_documents_val = TextSearchDocuments(connect_url='http://localhost:9200/')
            index_start_time = time()
            txt_index_documents_val.add_documents(docs_to_check_list=list_docs,
                                                  search_field_list=input_list_fields,
                                                  key=input_key,
                                                  object_type=input_object_type)
            print(f"indexing time: {time() - index_start_time}")
            search_start_time = time()
            res = txt_search_documents_val.check_for_text(object_type=input_object_type,
                                                          string_look_for=input_look_for, max_list_size=1000)
            print(f"searching time: {time() - search_start_time}")
            print(f"Result len: {len(res)}, list: \n{res}")
            #self.assertTrue(collections.Counter(res) == collections.Counter([i for i in range(0, amount_in)]))
            txt_index_documents_val.disconnect()
            txt_search_documents_val.disconnect()

            es = Elasticsearch(connect_url='http://localhost:9200/')
            index_in = f"{input_object_type}_index"
            for i in range(0, len(list_docs)):
                es.delete(index=index_in, id=i)
            es.transport.close()

    def tearDown(self) -> None:
        pass


if __name__ == '__main__':
    unittest.main()
