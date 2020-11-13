'''
input: keywords:キーワードのリスト   任意のクエリに改良する,docid:クエリのid
output: 上位100件の類似特許
'''
from elasticsearch import Elasticsearch
import json

def priorartsearch (keywords,docid):
    es = Elasticsearch("http://localhost:9200")
    index_name = 'clef_patent'
    write_file = '../result/test.prel'
    keywords_query = ' '.join(keywords)
    search_list = []
    _body = {
        "query": {
            "multi_match": {
                "fields": ["description.p"],
                "query": keywords_query
            }
       },
       "_source" : ["ucid","description"]
    }
    query = es.search(index=index_name, body=_body, size = 100,request_timeout=150)
    for i in range(100):
        search_list.append([query['hits']['hits'][i]['_source']['ucid'],query['hits']['hits'][i]['_score']])
    print(search_list)

    with open(write_file, mode='a') as f:
        for j,search in enumerate(search_list):
            text = str(docid)\
                + ' Q0 '\
                + str(search[0].replace( '-' , '' ))\
                + ' '\
                + str(j + 1)\
                + ' '\
                + str(search[1])\
                + ' STANDARD\n'
            f.write(text)
