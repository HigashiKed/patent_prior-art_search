from query_input.query_input import get_querydata
from keyword_extraction.keyword_extraction import get_keyword
from ela_search.ela_search import priorartsearch

if __name__ == '__main__':
    # clefもしくはntcirのクエリを整形
    param = 'clef'
    querydocument_list, docid_list, title_list = get_querydata(param)

    #1クエリ毎に上位keynum件のkeywordを得る
    para = 'TFIDF'  #TFIDFかMultipartiteRankの想定
    keynum = 100    #検索に使用する上位keynum件のキーワード
    for i, docid in enumerate(docid_list):
        #キーワード取得
        keywords = get_keyword(querydocument_list[i],para,keynum)
        tmp_keywords = []
        #elasticsearchのor検索は、キーワードをスペース区切りで並べるため改良
        for keyword in(keywords):
            tmp_keywords.append(keyword[0])
        print(tmp_keywords)
        exit()
        priorartsearch(tmp_keywords,docid)

  