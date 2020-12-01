'''
input: keywords:キーワードのリスト   任意のクエリに改良する,docid:クエリのid
output: 上位100件の類似特許
'''
from elasticsearch import Elasticsearch
import json
import ast

def priorartsearch_1 (tmp_keywords,docid,index_name,prel_file ):
    """
    全部orで繋いだクエリで検索
    """

    keywords_query = ' '.join(tmp_keywords)
    es = Elasticsearch("http://localhost:9200")
    write_file = prel_file+"_1.prel"
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
        for keyword in tmp_keywords[:2]:
            body_second += keyword+ " "
        body_second += '" ,"operator": "and"}}},'
        for keyword in tmp_keywords[2:]:
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

def priorartsearch_2 (tmp_keywords,docid,index_name,prel_file ,first_ave_score):

    es = Elasticsearch("http://localhost:9200")
    write_file = prel_file+"_2.prel"
    # 2つのみAND
    for main_keyword in tmp_keywords:
        print(main_keyword)
        """
        main_keywordを固定して検索する
        """
        not_main_keyword = [key for key in tmp_keywords if not key==main_keyword]
        for tmp_keyword in not_main_keyword:
            search_list = []
            body_first = '{"query": {"bool" : {"should" : ['
            body_second = ""
            body_second +='{ "match": { "text": { "query": "'
            body_second += main_keyword+ " "
            body_second += tmp_keyword
            body_second += '" ,"operator": "and"}}},'
            for keyword in [key for key in not_main_keyword if not key==tmp_keyword]:
                body_second +='{ "match": { "text": "'+ keyword +'" }},'
            body_third = ']}}}'
            _body = body_first+body_second+body_third
            _body = ast.literal_eval(_body)
            #print(json.dumps(_body,indent=2))
            query = es.search(index=index_name, body=_body, size = 100,request_timeout=150)
            sum_score = 0
            for i in range(len(query['hits']['hits'])):
                search_list.append([query['hits']['hits'][i]['_source']['ucid'],query['hits']['hits'][i]['_score']])
                sum_score += float(query['hits']['hits'][i]['_score'])
                #print(json.dumps(query['hits']['hits'][i],indent=2))
            #print(search_list)
            ave_score = sum_score/len(search_list)
            print(ave_score)
            if first_ave_score < ave_score:
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
                break
        else:
            continue
        break
    else:
        #一番最初が一番よかったもの
        search_list = []
        keywords_query = ' '.join(tmp_keywords)
        _body = '{"query": {"match": {"text": "'+ keywords_query + '"}},"_source" : ["ucid","text"]}'
        query = es.search(index=index_name, body=_body, size = 100,request_timeout=150)
        sum_score = 0
        for i in range(len(query['hits']['hits'])):
            search_list.append([query['hits']['hits'][i]['_source']['ucid'],query['hits']['hits'][i]['_score']])
            sum_score += float(query['hits']['hits'][i]['_score'])
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