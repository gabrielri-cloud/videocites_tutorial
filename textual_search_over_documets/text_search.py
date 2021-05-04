
from elasticsearch import Elasticsearch


class text_search_over_documents:

    def __init__(self, connect_url, object_type):
        self.es = Elasticsearch(connect_url)
        # TODO decide on a name
        self.index_in = f"{object_type}_index"

    def add_documents(self, docs_to_check_list, search_field_list, key):
        praised_text = ""
        for i in range(0, len(docs_to_check_list)):
            for field in search_field_list:
                # TODO check if fields exist
                split_by_point_list = field.split(".")
                tmp_json_or_text = docs_to_check_list[i].get(split_by_point_list[0])
                if len(split_by_point_list) > 1:
                    for j in range(1, len(split_by_point_list)):
                        tmp_json_or_text = tmp_json_or_text.get(split_by_point_list[j])
                # TODO decide split char
                praised_text += " " + tmp_json_or_text

            search_doc = {
                'key': docs_to_check_list[i].get(key),
                'text': praised_text
            }

            res = self.es.index(index=self.index_in, id=i, body=search_doc)
            if res['result'] != 'updated':
                # TODO decide what to do if not updated
                print('Error - not updated')

    def check_for_text(self, string_look_for):
        query_body = {
            "query": {
                "match": {
                    'text': f'{string_look_for}'
                }
            }
        }

        search_result_list = self.es.search(index=self.index_in, body=query_body)

        keys_result_list = []
        for search_result in search_result_list['hits']['hits']:
            keys_result_list.append(search_result['_source']['key'])
        return keys_result_list
