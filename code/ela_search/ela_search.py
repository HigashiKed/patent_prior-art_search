'''
input: keywords:キーワードのリスト   任意のクエリに改良する,docid:クエリのid
output: 上位100件の類似特許
'''
from elasticsearch import Elasticsearch
import json
import ast

def priorartsearch (tmp_keywords,docid,index_name):

    keywords_query = ' '.join(tmp_keywords)
    es = Elasticsearch("http://localhost:9200")
    write_file = '../result/TFiDF_AND_4.prel'
    search_list = []
    if index_name=="clef_patent":
        _body = {
            "query": {
                "multi_match": {
                    "fields": ["description.p"],
                    "query": keywords_query
                }
            },
            "_source" : ["ucid","description"]
        }
    elif index_name=="clef_text":
        """
        _body = '{"query": {"match": {"text": "'+ keywords_query + '"}},"_source" : ["ucid","text"]}'
        """
        
        #should句のみ
        _body = '{"query": {"match": {"text": "'+ keywords_query + '"}},"_source" : ["ucid","text"]}'
        """
        
        # 2つのみAND
        
        body_first = '{"query": {"bool" : {"should" : ['
        body_second = ""
        body_second +='{ "match": { "text": { "query": "'
        for keyword in tmp_keywords[:5]:
            body_second += keyword+ " "
        body_second += '" ,"operator": "and"}}},'
        for keyword in tmp_keywords[5:]:
            body_second +='{ "match": { "text": "'+ keyword +'" }},'
        body_third = ']}}}'
        _body = body_first+body_second+body_third
        _body = ast.literal_eval(_body)
        #print(json.dumps(_body,indent=2))
        """
        
        
        """
        #shouldとmust
        body_first = '{"query": {"bool" : {"must" : ['
        body_second = ""
        for keyword in tmp_keywords:
            body_second +='{ "match": { "text": "'+ keyword +'" }},'
        body_third = '],"should" : ['
        body_fourth = ""
        for keyword in tmp_keywords[1:]:
            body_fourth +='{ "match": { "text": "'+ keyword +'" }},'
        
        body_fifth = ']}}}'
        _body = body_first+body_second+body_third+body_fourth+body_fifth
        _body = ast.literal_eval(_body)
        """

    query = es.search(index=index_name, body=_body, size = 100,request_timeout=150)
    sum_score = 0
    for i in range(len(query['hits']['hits'])):
        search_list.append([query['hits']['hits'][i]['_source']['ucid'],query['hits']['hits'][i]['_score']])
        sum_score += float(query['hits']['hits'][i]['_score'])
        #print(json.dumps(query['hits']['hits'][i],indent=2))
    #print(search_list)
    ave_score = sum_score/len(search_list)
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
    return ave_score
