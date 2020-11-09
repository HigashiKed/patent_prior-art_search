'''
input: text:文章 para:キーワード抽出手法 TFIDFかMultipartiteを想定, keynum:上位keynum件のキーワードをreturn
output: 上位keynum件のキーワード
'''

from elasticsearch import Elasticsearch
import re
import json
import collections

def get_keyword (text,para,keynum):
    #print(text)
    es = Elasticsearch("http://localhost:9200")
    words = split_stem(text,es)
    words_fix = collections.Counter(words)  #重複数カウント
    values, counts = zip(*words_fix.most_common())  #valuesは重複削除したワード,countsは重複削除した出現回数
    values_idf = []
    #100単語ずつ検索
    for i in range(int(len(values)/100)+1):
        if i == len(values)/100 :
            # 最後の要素は100個以下なため別処理
            tmp_values = values[i*100 :]
        else:
            tmp_values = values[i*100 : (i+1)*100-1]

        words_query = mojiretu = ' '.join(tmp_values)
        #今は "description.DETAILED_DESC.plain"で。いずれ、"description.BRIEF_SUMMARY.plain", "description.DETAILED_DESC.plain"]

        _body = {
            "query": {
                "multi_match": {
                    "fields": ["description.DETAILED_DESC.plain"],
                    "query": words_query 
                }
        }
        }
        # IDFを求めるために適当なdocidのクエリを指定
        query = es.search(index='clef', body=_body, size=1, request_timeout=150)
        docid = query['hits']['hits'][0]["_source"]["documentId"]

        query = es.explain(index='clef', id = docid,body=_body, request_timeout=150)
    
        #print(json.dumps(query['explanation'],indent=2))
        for j in range(len(query['explanation']["details"])):
            tmp = query['explanation']["details"][j]["description"]
            word = tmp[tmp.find(":")+1 : tmp.find(' ')]
            idf = query['explanation']["details"][j]["details"][0]["details"][1]["value"]
            values_idf.append((word,idf))
        print(values_idf)

        exit()


    #print(json.dumps(query['explanation']['details'][0]['details'][0], indent=2))
    #print(query['explanation']['details'][0]['details'][1]['value'])    #idf値
    #print(query['explanation']['details'][0]['details'][0]['details'][1]['value'])    #idf値
    #doc_id = query['hits']['hits'][0]['_id']
    #query = es.explain(index='clef', body=_body, id=doc_id)


    return ["a","b","c","d"]

    


def split_stem(text,es):
    """
    inputされた文を語幹に分割してSTEMフォルダにpickleで保存
    """

    # elasticsearch analyzerのmax_token_sizeは10000なので,まずsentenceに分割してからstemmingする

    sentences = text.split(".")  #カンマ区切りで分割

    print(sentences)
    words = []  #textに出力する文字のstem一覧
    for sen in enumerate(sentences):
        x = "stemming" # stemming / no_stemming
        if x == "stemming":
            #stemmingする時
            stem_list = []
            _body = {"analyzer": "english", "text": sen}
            query = es.indices.analyze(body=_body)
            for token_info in query['tokens']:
                tmp = (re.sub(r'[0-9]+', "0", token_info['token']))   # 数値や空白は0に置換して語幹をsterm_listへ
                stem_list.append(tmp)
            stem_list = [n for n in stem_list if n!='0']    #0を削除
            #print(stem_list)
            words.extend(stem_list)
        elif x == "no_stemming":
            #stemmingしない時　単純にスペース区切りのstopwords除去
            """
            tmp = sen.split(" ")
            tmp = (re.sub(r'[0-9]+', "0", tmp))
            """
            stem_list = []
            _body = {"analyzer": "english", "text": sen}
            query = es.indices.analyze(body=_body)
            print(query)
            #_body = {"analyzer": {"rebuilt_english": {"tokenizer":  "standard","filter": ["lowercase","english_stop","english_keywords"]}}, "text": sen}
            #query = es.indices.analyze(body=mapping)
            for token_info in query['tokens']:
                tmp = (re.sub(r'[0-9]+', "0", token_info['token']))   # 数値や空白は0に置換して語幹をsterm_listへ
                stem_list.append(tmp)
            stem_list = [n for n in stem_list if n!='0']    #0を削除
            words.extend(stem_list)

    return words
