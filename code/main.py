from query_input.query_input import get_querydata
from keyword_extraction.keyword_extraction import get_keyword
from ela_search.ela_search import priorartsearch
import time
from tqdm import tqdm
import json

if __name__ == '__main__':
    # clefもしくはntcirのクエリを整形
    start = time.time()
    param = 'clef'
    querydocument_list, docid_list, title_list = get_querydata(param)
    keywordlist_memo = "keyword_list_AND_4.txt"
    #index_name = "clef_patent"  # description,abstract,claimsに分かれている
    index_name = "clef_text"  # 全てが一つのテキストになっている

    #1クエリ毎に上位keynum件のkeywordを得る
    para = 'TFIDF'  #TFIDFかMultipartiteRankの想定
    keynum = 100    #検索に使用する上位keynum件のキーワード
    for i, docid in tqdm(enumerate(docid_list)):
        #キーワード取得
        keywords = get_keyword(querydocument_list[i],para,keynum,index_name)
        print(keywords)
        tmp_keywords = []
        #elasticsearchのor検索は、キーワードをスペース区切りで並べるため改良
        for keyword in(keywords):
            tmp_keywords.append(keyword[0])
        with open(keywordlist_memo, mode='a') as f:
            f.write(docid+"\n"+str(tmp_keywords))
            f.write("\n")
        first_ave_score = priorartsearch(tmp_keywords,docid,index_name)
        print(first_ave_score)
 
    process_time = time.time() - start
    print("実行時間")
    print(process_time)
  