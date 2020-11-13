'''
input: text:文章 para:キーワード抽出手法 TFIDFかMultipartiteを想定, keynum:上位keynum件のキーワードをreturn
output: 上位keynum件のキーワード
'''

from elasticsearch import Elasticsearch
import re
import json
import collections
from operator import itemgetter

def get_keyword (text,para,keynum):
    #print(text)
    es = Elasticsearch("http://localhost:9200")
    index_name = 'clef_patent'
    words = split_stem(text,es)
    words_fix = collections.Counter(words)  #重複数カウント
    values, counts = zip(*words_fix.most_common())  #valuesは重複削除したワード,countsは重複削除した出現回数
    values_idf = []
    #100単語ずつ検索
    for i in range(int(len(values)/100)+1):
        prev_docid = ""
        copy_values = values
        flg = True
        if i == len(copy_values)/100 :
            # 最後の要素は100個以下なため別処理
            tmp_values = copy_values[i*100 :]
        else:
            tmp_values = copy_values[i*100 : (i+1)*100-1]
        while flg:
            if len(tmp_values)==0:
                flg = False
                continue
            #検索上位1件にhitした特許に含まれる文字列のみしかidf求められないのでループ
            
            words_query = ' '.join(tmp_values)
            #今は "description.p"で。いずれ、"abstract.p", "claim.text"]
            _body = {
                "query": {
                    "multi_match": {
                        "fields": ["description.p"],
                        "query": words_query 
                    }
                }
            }
            # IDFを求めるために適当なdocidのクエリを指定
            #何もhitしなかった時の条件を追加する
            query = es.search(index=index_name, body=_body, size=1, request_timeout=150)
            try:
                docid = query['hits']['hits'][0]["_source"]["ucid"]
                if docid == prev_docid:
                    break
            except IndexError:
                break
            prev_docid = docid
            query = es.explain(index=index_name, id = docid,body=_body, request_timeout=150)
 
            for j in range(len(query['explanation']["details"])):
                #print(tmp_values)
                #if len(tmp_values)!=1:
                #残り1単語の時
                try:
                    tmp = query['explanation']["details"][j]["description"]
                    idf = query['explanation']["details"][j]["details"][0]["details"][1]["value"]
                except IndexError:
                    tmp = query['explanation']["description"]
                    idf = query["explanation"]["details"][0]["details"][1]["value"]
                word = tmp[tmp.find(":")+1 : tmp.find(' ')]
                values_idf.append((word,idf))
                # idfが得られたwordを削除
                #print(word)
                #print(tmp_values)
                l = list(tmp_values)
                try:
                    l.remove(word)
                except ValueError:
                    pass
                tmp_values = tuple(l)
    #print(values_idf)
    #tf 算出 tf = freq / (freq + k1 * (1 - b + b * dl / avgdl
    """
    "freq, occurrences of term within document"
    "k1, term saturation parameter"=1.2
    "b, length normalization parameter"=0.75
    "dl, length of field (approximate)"=フィールドの長さ=textの長さ
    "avgdl, average length of field"=2865.817 !!全部入力後要変更
    """
    k1 = 1.2
    b = 0.75
    dl = len(text)
    avgdl = 2865.817 

    tfidf_data = []
    for value in(values_idf):
        #各単語ごと
        freq = words_fix[value[0]]
        tf = freq / (freq + k1 * (1 - b + b * dl / avgdl))
        tmp = (value[0], tf, value[1], tf*value[1])
        tfidf_data.append(tmp)
    tdfidf_data = sorted(tfidf_data, key=itemgetter(3),reverse=True)    #tfidf順にソート






    #print(json.dumps(query['explanation']['details'][0]['details'][0], indent=2))
    #print(query['explanation']['details'][0]['details'][1]['value'])    #idf値
    #print(query['explanation']['details'][0]['details'][0]['details'][1]['value'])    #idf値
    #doc_id = query['hits']['hits'][0]['_id']
    #query = es.explain(index='clef', body=_body, id=doc_id)


    return (tfidf_data[:100])


    


def split_stem(text,es):
    """
    inputされた文を語幹に分割してSTEMフォルダにpickleで保存
    """

    # elasticsearch analyzerのmax_token_sizeは10000なので,まずsentenceに分割してからstemmingする

    sentences = text.split(".")  #カンマ区切りで分割

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
            #_body = {"analyzer": {"rebuilt_english": {"tokenizer":  "standard","filter": ["lowercase","english_stop","english_keywords"]}}, "text": sen}
            #query = es.indices.analyze(body=mapping)
            for token_info in query['tokens']:
                tmp = (re.sub(r'[0-9]+', "0", token_info['token']))   # 数値や空白は0に置換して語幹をsterm_listへ
                stem_list.append(tmp)
            stem_list = [n for n in stem_list if n!='0']    #0を削除
            words.extend(stem_list)

    return words
