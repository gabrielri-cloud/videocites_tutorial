from datetime import datetime
#from text_index_documents import TextIndexDocuments
from text_index_documents_threads import TextIndexDocuments
from text_search_over_documets import TextSearchDocuments
import time

if __name__ == '__main__':
    input_doc = {
        'id': 200,
        'author': 'kimchy',
        'title1': 'heee: gogiga. bonsai cool.',
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
        'title1': 'heeey: gogigaa. bonsai cool.',
        'title2': 'Elasticsearch: cool. bonsai c.',
        'timestamp': datetime.now(),
        'nested1': {
            'nested2': 'eaeeea',
        },
        'desc': 'khbuvkjb'
    }
    list_docs = [input_doc, input_doc2]
    list_docs = []
    for i in range(0, 1):
        list_docs.append({
            'id': 3 * i,
            'author': 'kimchy',
            'title1': 'heeey: gogigaa. bonsai cool.',
            'title2': 'Elasticsearch: cool. bonsai c.',
            'timestamp': datetime.now(),
            'nested1': {
                'nested2': 'eaeeea',
            },
            "keywords": [
                {
                    "id": "eyJ0ZXh0IjogIlRvbSBCcmFkeSBhZHZlcnRpc2VtZW50In0=",
                    "text": " Brady advertisement",
                    "tags": [
                        {
                            "text": " Brady advertisement"
                        },
                    ]
                },
                {
                    "id": "eyJ0ZXh0IjogIlRvbSBCcmFkeSBjb21tZXJjaWFsIn0=",
                    "text": "TOM Brady commercial",
                    "tags": [
                        {
                            "text": " Tom Brady advertisement"
                        },
                    ]
                },
                {
                    "id": "eyJ0ZXh0IjogIlRvbSBCcmFkeSBhZHZlcnRpc2VtZW50In0=",
                    "text": " Brady advertisement",
                    "tags": []
                },
            ],
            'desc': 'khbuvkjb'
        })
    print(len(list_docs))
    input_key = 'id'
    input_look_for = 'ToM'
    input_list_fields = ['title1', 'title2', 'nested1.nested2', 'keywords.tags.text']
    input_object_type = 1
    start_time = time.time()
    txt_index_documents_val = TextIndexDocuments(connect_url='http://localhost:9200/')
    txt_search_documents_val = TextSearchDocuments(connect_url='http://localhost:9200/')

    txt_index_documents_val.add_documents(docs_to_check_list=list_docs,
                                          search_field_list=input_list_fields,
                                          key=input_key,
                                          object_type=input_object_type)

    res = txt_search_documents_val.check_for_text(object_type=input_object_type,
                                                  string_look_for=input_look_for, max_list_size=10)
    print("--- %s seconds ---" % (time.time() - start_time))
    print(res)

