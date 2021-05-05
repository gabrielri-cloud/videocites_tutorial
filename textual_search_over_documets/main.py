from datetime import datetime
from text_index_documents import TextIndexDocuments
from text_search_over_documets import TextSearchDocuments

if __name__ == '__main__':
    input_doc = {
        'id': 200,
        'author': 'kimchy',
        'title1': 'heee: gogi. bonsai cool.',
        'title2': 'Elasticsearch: cool. bonsai c.',
        'timestamp': datetime.now(),
        'nested1': {
            'nested2': 'e',
        },
        'desc': 'khbuvkjb'
    }
    input_doc2 = {
        'id': 201,
        'author': 'kimchy',
        'title1': 'heeey: gogig. bonsai cool.',
        'title2': 'Elasticsearch: cool. bonsai c.',
        'timestamp': datetime.now(),
        'nested1': {
            'nested2': 'eaeee',
        },
        'desc': 'khbuvkjb'
    }
    input_key = 'id'
    input_look_for = 'heeey'
    input_list_fields = ['title1', 'title2', 'nested1.nested2']
    input_object_type = 1

    txt_index_documents_val = TextIndexDocuments(connect_url='http://localhost:9200/')
    txt_search_documents_val = TextSearchDocuments(connect_url='http://localhost:9200/')

    txt_index_documents_val.add_documents(docs_to_check_list=[input_doc, input_doc2],
                                          search_field_list=input_list_fields,
                                          key=input_key,
                                          object_type=input_object_type)

    res = txt_search_documents_val.check_for_text(object_type=input_object_type,
                                                  string_look_for=input_look_for)
    print(res)
