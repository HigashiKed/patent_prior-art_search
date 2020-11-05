'''
input: text:文章 para:キーワード抽出手法 TFIDFかMultipartiteを想定, keynum:上位keynum件のキーワードをreturn
output: 上位keynum件のキーワード
'''

from elasticsearch import Elasticsearch
import re

def get_keyword (text,para,keynum):
    #print(text)
    words = split_stem(text)
    return ["a","b","c","d"]


def split_stem(text):
    # inputされた文を語幹に分割してSTEMフォルダにpickleで保存
    es = Elasticsearch("http://localhost:9200")

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
    '''
    # elasticsearch analyzerのmax_token_sizeは10000なので,スペース9000区切りで分割する
    stem_list = []
    _body = {"analyzer": "english", "text": sentence}
    query = es.indices.analyze(body=_body)
    for token_info in query['tokens']:
        tmp = (re.sub(r'[0-9]+', "0", token_info['token']))   # 数値は0に置換して語幹をsterm_listへ
        stem_list.append(tmp)

    return stem_list
    '''