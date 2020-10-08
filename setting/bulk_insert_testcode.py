"""
ローカルのelasticsearchに特許データ挿入するためのテストコード
aws elasticsearchに特許データを挿入するためのコード->bulk_insert.py
"""

import json
from pathlib import Path

from elasticsearch import Elasticsearch
from tqdm import tqdm

import gzip

if __name__ == '__main__':    
    #es = Elasticsearch("https://vpc-onilab-fxtaxuoytzkyfehmaqhoryzfoi.ap-northeast-1.es.amazonaws.com:443")
    es = Elasticsearch("http://localhost:9200")
    index = 'us'
    
    files = list(Path('/Users/higashi/us_data/').glob(r'*.bulk.gz'))

    #10個に分割
    splitted_files = []
    tmp_list = []

    for i,f in enumerate(files):
        if (i<=587):
            pass
        elif (i%10)==9:
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

    print(splitted_files)
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
            with gzip.open(f,mode = 'rt') as fp:
                lst = [json.loads(s) for s in fp.read().splitlines()]
    
            a = []
            for j,l in enumerate(lst):
    
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
