from datetime import datetime
from text_search import text_search_over_documents


if __name__ == '__main__':
    input_doc = {
        'id': 100,
        'author': 'kimchy',
        'title1': 'Elasticsearch: dogi. bonsai cool.',
        'title2': 'Elasticsearch: cool. bonsai c.',
        'timestamp': datetime.now(),
        'nested1': {
            'nested2': 'e',
        },
        'desc': 'khbuvkjb'
    }
    input_doc2 = {
        'id': 101,
        'author': 'kimchy',
        'title1': 'Elasticsearcha: dogig. bonsai cool.',
        'title2': 'Elasticsearch: cool. bonsai c.',
        'timestamp': datetime.now(),
        'nested1': {
            'nested2': 'eaeee',
        },
        'desc': 'khbuvkjb'
    }
    input_key = 'id'
    input_look_for = 'eaeee'
    input_list_fields = ['title1', 'title2', 'nested1.nested2']
    input_object_type = 1

    txt_search_documents_val = text_search_over_documents(connect_url='http://localhost:9200/',
                                                          object_type=input_object_type)

    txt_search_documents_val.add_documents(docs_to_check_list=[input_doc, input_doc2],
                                           search_field_list=input_list_fields,
                                           key=input_key)

    res = txt_search_documents_val.check_for_text(string_look_for=input_look_for)
    print(res)
