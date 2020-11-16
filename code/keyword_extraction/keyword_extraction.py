'''
input: text:文章 para:キーワード抽出手法 TFIDFかMultipartiteを想定, keynum:上位keynum件のキーワードをreturn
output: 上位keynum件のキーワード
'''

from elasticsearch import Elasticsearch
import re
import json
import collections
from operator import itemgetter
import nltk

def get_keyword (text,para,keynum,index_name):
    #print(text)
    es = Elasticsearch("http://localhost:9200")
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
            print(len(tmp_values))
            for tmp_word in tmp_values:
                if index_name=="clef_patent":
                    #今は "description.p"で。いずれ、"abstract.p","claims.claim"]
                    _body = {
                        "query": {
                            "multi_match": {
                                "fields": ["description.p"],
                                "query": tmp_word
                            }
                        }
                    }
                elif index_name=="clef_text":
                    _body = {
                        "query": {
                            "match": {
                                "text" : tmp_word
                            }
                        }
                    }
            

                # IDFを求めるために適当なdocidのクエリを指定
                #何もhitしなかった時の条件を追加する
                query = es.search(index=index_name, body=_body, size=1, request_timeout=150)
                try:
                    docid = query['hits']['hits'][0]["_source"]["ucid"]
                except IndexError:
                    l = list(tmp_values)
                    l.remove(tmp_word)
                    tmp_values = tuple(l)
                    print("IndexError")
                    continue
                query = es.explain(index=index_name, id = docid,body=_body, request_timeout=150)
            
                tmp = query['explanation']["description"]
                idf = query['explanation']["details"][0]["details"][1]["value"]
                word = tmp[tmp.find(":")+1 : tmp.find(' ')]
                values_idf.append((tmp_word,idf))  #analyzeされる前の単語
                # idfが得られたwordを削除
                #print(word)
                #print(tmp_values)
                l = list(tmp_values)
                try:
                    l.remove(tmp_word)
                except ValueError:
                    print("Valueerror")
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
    if index_name=="clef_patent":
        avgdl = 2865.817 
    elif index_name=="clef_text":
        avgdl = 2223.4998
        

    tfidf_data = []
    for value in(values_idf):
        #各単語ごと
        freq = words_fix[value[0]]
        tf = freq / (freq + k1 * (1 - b + b * dl / avgdl))
        tmp = (value[0], tf, value[1], tf*value[1])
        tfidf_data.append(tmp)
    #tfidf_data = sorted(tfidf_data, key=itemgetter(3),reverse=True)    #tfidf順にソート
    #return (tfidf_data[:100])

    tf_data = sorted(tfidf_data, key=itemgetter(1),reverse=True)    #tfidf順にソート
    return (tf_data[:100])


    


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
                if tmp not in stopwords():  # stopword除去
                    if not len(tmp)==1:
                        #1文字は除外
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
            for token_info in query['tokens']:
                tmp = (re.sub(r'[0-9]+', "0", token_info['token']))   # 数値や空白は0に置換して語幹をsterm_listへ
                if tmp not in stopwords():  # stopword除去
                    stem_list.append(tmp)
            stem_list = [n for n in stem_list if n!='0']    #0を削除
            words.extend(stem_list)

    return words

def stopwords():
    #nltk.download('stopwords')
    symbols = ["'", '"', '`', '.', ',', '-', '!', '?', ':', ';', '(', ')', '*', '--', '\\']
    stopwords = nltk.corpus.stopwords.words('english')
    numeric = ['0']
    return stopwords + symbols + numeric

def explain_patent(keyword_list, index_name, es):
    """
    類似特許検索 IDF などの詳細を持つクエリを得る
    """

    if index_name=="clef_patent":
        #今は "description.p"で。いずれ、"abstract.p","claims.claim"]
        _body = {
            "query": {
                "multi_match": {
                    "fields": ["description.p"],
                    "query": keyword_list
                }
            }
        }
    elif index_name=="clef_text":
        _body = {
            "query": {
                "match": {"text" : keyword_list }
            }
        }
    query = es.search(index=index_name, body=_body, size=1, request_timeout=150)

    doc_id = query['hits']['hits'][0]['_id']
    query = es.explain(index=index_name, body=_body, id=doc_id)

    return query