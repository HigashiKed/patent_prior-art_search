"""
jsonファイルのうち全部をelasticsearchに挿入する必要はないので
必要な部分を抜き出してinsertする
"""

import json
from pathlib import Path

from elasticsearch import Elasticsearch
from tqdm import tqdm

import gzip

import copy

def del_dic(data, keys):
    if len(keys) == 1:
        del data[keys[0]]
        return
    else:
        temp_key = keys.pop(0)
        temp_data = data.pop(temp_key)
        del_dic(temp_data, keys)
        data[temp_key] = temp_data

def del_dic_main(dic_data, del_list):
    work_data = copy.copy(dic_data)
    del_keys = copy.copy(del_list)
    del_keys.sort(reverse=True)
    for key in del_keys:
        key_list = key.split(".")
        if len(key_list) >= 1:
            del_dic(work_data, key_list)  
    return work_data


if __name__ == '__main__':    
    
    #es = Elasticsearch("https://vpc-onilab-fxtaxuoytzkyfehmaqhoryzfoi.ap-northeast-1.es.amazonaws.com:443")
    es = Elasticsearch("http://localhost:9200")
    index = 'us'

    
    files = list(Path('/Users/higashi/us_data/').glob(r'*.bulk.gz'))

    #10個に分割
    splitted_files = []
    tmp_list = []

    for i,f in enumerate(files):
        if (i%10)==9:
            #古いリストをsplitted_filesに入れる
            splitted_files.append(tmp_list)
            #リストを新しく作ってファイルを入れる
            tmp_list = []
            tmp_list.append(f)
            
        else:
            #リストにファイルを入れる
            tmp_list.append(f)
    else:
        if len(tmp_list) != 0:
            splitted_files.append(tmp_list)
            tmp_list = []

    settings = {
        'index': {
            'similarity': {
                'default': {
                    'type': 'LMDirichlet'
                }
            }
        }
    }

    #es.indices.delete(index='us')

    #es.indices.create(
    #    index=index,
    #    body={ 'settings': settings }
    #)

    for i,flist in enumerate(splitted_files):
        #解凍
        for f in tqdm(flist):
            # ファイルをオープンする
            test_data = open("inserted.txt", "r")

            # 行ごとにすべて読み込んでリストデータにする
            lines = test_data.readlines()
            #print(lines[0])
            if(str(f)+"\n" in lines):
                print(str(f))
                continue

            with open("inserted.txt", mode='a') as fa:
                fa.write(str(f)+"\n")

            with gzip.open(f,mode = 'rt') as fp:
                lst = [json.loads(s) for s in fp.read().splitlines()]
    
            a = []
            for j,l in enumerate(lst):
                del l["abstract"]["raw"],l["abstract"]["normalized"],l["classification"]["cpc"]
                #claim treeのraw削除
                for k in range(len(l["claims"])):
                    del l["claims"][k]["raw"],l["claims"][k]["normalized"]
                del l["description"]["DRAWING_DESC"]["raw"],l["description"]["DRAWING_DESC"]["normalized"]
                
                #print(json.dumps(l, indent=2))

                a.append('{ "index" : { "_index" : "' + index + '"}, "_type": "_doc" }')
                a.append(l)
                if (j%25)==24:
                    try:
                        es.bulk(index=index, doc_type='_doc', body=a)
                        a = []
                    except:
                        continue

            else:
                if len(a) != 0:
                    es.bulk(index=index, doc_type='_doc', body=a)
                    a = []
        print("\nfinished:")
        print(flist)
