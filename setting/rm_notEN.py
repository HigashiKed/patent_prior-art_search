'''
英語で書かれていないものを削除する
'''
from elasticsearch import Elasticsearch
import json

if __name__ == '__main__':
    es = Elasticsearch("http://localhost:9200")
    index_name = 'clef_patent'
    _body = {"query":{"bool": {"must_not": {"term": {"lang.keyword":"EN"}}}},"_source": ["ucid","lang"]}
    bulk = ""
    query = es.search(index=index_name, body=_body, scroll = '3m',size=10000)
    print(json.dumps(query,indent=2))
    #print ("total docs:", len(query["hits"]["hits"]))
    scroll_id = query['_scroll_id']
    for hits in query["hits"]["hits"]:
        bulk = bulk + "{ \"delete\" : { \"_index\" : \"" + str(hits["_index"]) + "\", \"_type\" : \"" + str(hits["_type"]) + "\", \"_id\" : \"" + str(hits["_id"]) + "\" } }\n"
    es.bulk( body=bulk )
    flg = True
    docid_list = []
    while flg:
        bulk = ""
        query = es.scroll(scroll_id = scroll_id,scroll = '1s')
        #print(json.dumps(query,indent=2))
        print ("total docs:", len(query["hits"]["hits"]))
        if(len(query["hits"]["hits"])==0):
            flg=False
        
        for hits in query["hits"]["hits"]:
            docid = hits["_source"]["ucid"]
            #print(docid)
            docid_list.append(docid)
            bulk = bulk + "{ \"delete\" : { \"_index\" : \"" + str(hits["_index"]) + "\", \"_type\" : \"" + str(hits["_type"]) + "\", \"_id\" : \"" + str(hits["_id"]) + "\" } }\n"
            #es.delete(index=index_name, id=docid)
        
        scroll_id = query['_scroll_id']
        es.bulk( body=bulk )
    #print(docid_list)
    #print(bulk)
    