'''
input: text:文章 para:キーワード抽出手法 TFIDFかMultipartiteを想定, keynum:上位keynum件のキーワードをreturn
output: 上位keynum件のキーワード
'''

from elasticsearch import Elasticsearch

def get_keyword (text,para,keynum):
    split_stem(text)
    return ["a","b","c","d"]


def split_stem(sentence):
    # inputされた文を語幹に分割してSTEMフォルダにpickleで保存
    es = Elasticsearch("http://localhost:9200")
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