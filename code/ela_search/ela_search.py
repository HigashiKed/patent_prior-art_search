'''
input: keywords:キーワードのリスト   任意のクエリに改良する
output: 上位100件の類似特許
'''
from elasticsearch import Elasticsearch

def priorartsearch (keywords):
    es = Elasticsearch("http://localhost:9200")
    index_name = 'clef'
    keywords_query = ' '.join(keywords)
    docid_list = []
    _body = {
        "query": {
            "multi_match": {
                "fields": [ "description.BRIEF_SUMMARY.plain", "description.DETAILED_DESC.plain"],
                "query": keywords_query
            }
       },
       "_source" : ["documentId","title","description"]
    }
    query = es.search(index=index_name, body=_body, size = 100,request_timeout=150)
    for i in range(100):
        print(query['hits']['hits'][i]['_source']['documentId'])
        docid_list.append(query['hits']['hits'][i]['_source']['documentId'])
    print(docid_list)
    exit()
    """
    with open('../result/' + param + '/stopword_fixed/' + param + '_keynum' + str(keynum) + '_simnum' + str(simnum) + '.prel', 1'a') as f:
        for j in range(simnum):
            text = str(docid_list[int(args[1]) + i])\
                + ' Q0 '\
                + str(query['hits']['hits'][j]['_source']['docid'])\
                + ' '\
                + str(j + 1)\
                + ' '\
                + str(query['hits']['hits'][j]['_score'])\
                + ' STANDARD\n'
            f.write(text)
    """