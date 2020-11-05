from query_input.query_input import get_querydata
from keyword_extraction.keyword_extraction import get_keyword

if __name__ == '__main__':
    # clefもしくはntcirのクエリを整形
    param = 'clef'
    querydocument_list, docid_list, title_list = get_querydata(param)

    #1クエリ毎に上位keynum件のkeywordを得る
    para = 'TFIDF'  #TFIDFかMultipartiteRankの想定
    keynum = 100    #検索に使用する上位keynum件のキーワード
    for i, docid in enumerate(docid_list):
        keywords = get_keyword(querydocument_list[i],para,keynum)
        print(keywords)
        exit()