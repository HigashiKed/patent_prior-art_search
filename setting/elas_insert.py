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
    
    es = Elasticsearch("http://localhost:9200")
    index_name = 'clef'
    
    es.indices.delete(index=index_name)

    
    mapping = {
        "mappings": {
            "properties" : {
                "abstract" : {
                    "properties" : {
                        "plain" : {
                            "type" : "text",
                            "fields" : {
                                "keyword" : {
                                    "type" : "keyword",
                                    "ignore_above" : 256
                                },
                                "english" : {
                                    "analyzer": "english",
                                    "type" : "text"
                                }
                            }
                        }
                    }
                },
                "applicationDate" : {
                    "properties" : {
                        "iso" : {
                            "type" : "date"
                        },
                        "raw" : {
                            "type" : "text",
                            "fields" : {
                                "keyword" : {
                                    "type" : "keyword",
                                    "ignore_above" : 256
                                }
                            }   
                        }
                    }
                },
                "applicationId" : {
                    "type" : "text",
                    "fields" : {
                        "keyword" : {
                            "type" : "keyword",
                            "ignore_above" : 256
                        }
                    }
                },
                "claims" : {
                    "properties" : {
                        "claimTree" : {
                            "properties" : {
                                "childCount" : {
                                    "type" : "long"
                                },
                                "claimTreelevel" : {
                                    "type" : "long"
                                },
                                "parentCount" : {
                                    "type" : "long"
                                }
                            }
                        },
                        "id" : {
                            "type" : "text",
                            "fields" : {
                                "keyword" : {
                                    "type" : "keyword",
                                    "ignore_above" : 256
                                }
                            }
                        },
                        "plain" : {
                            "type" : "text",
                            "fields" : {
                                "keyword" : {
                                    "type" : "keyword",
                                    "ignore_above" : 256
                                },
                                "english" : {
                                    "analyzer": "english",
                                    "type" : "text"
                                }
                            }
                        }
                    }
                },
                "classifications" : {
                    "properties" : {
                        "ipc" : {
                            "properties" : {
                                "facets" : {
                                    "type" : "text",
                                    "fields" : {
                                        "keyword" : {
                                            "type" : "keyword",
                                            "ignore_above" : 256
                                        }
                                    }
                                },
                                "normalized" : {
                                    "type" : "text",
                                    "fields" : {
                                        "keyword" : {
                                            "type" : "keyword",
                                            "ignore_above" : 256
                                        }   
                                    }
                                }
                            }
                        }
                    }
                },
                "description" : {
                    "properties" : {
                        "BRIEF_SUMMARY" : {
                            "properties" : {
                                "plain" : {
                                    "type" : "text",
                                    "fields" : {
                                        "keyword" : {
                                            "type" : "keyword",
                                            "ignore_above" : 256
                                        },
                                        "english" : {
                                            "analyzer": "english",
                                            "type" : "text"
                                        }
                                    }
                                }
                            }
                        },
                        "DETAILED_DESC" : {
                            "properties" : {
                                "plain" : {
                                    "type" : "text",
                                    "fields" : {
                                    "keyword" : {
                                            "type" : "keyword",
                                            "ignore_above" : 256
                                        },
                                        "english" : {
                                            "analyzer": "english",
                                            "type" : "text"
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "documentDate" : {
                    "properties" : {
                        "iso" : {
                            "type" : "date"
                        },
                        "raw" : {
                            "type" : "text",
                            "fields" : {
                                "keyword" : {
                                    "type" : "keyword",
                                    "ignore_above" : 256
                                }
                            }
                        }
                    }
                },
                "documentId" : {
                    "type" : "text",
                    "fields" : {
                        "keyword" : {
                            "type" : "keyword",
                            "ignore_above" : 256
                        }
                    }
                },
                "patentCorpus" : {
                    "type" : "text",
                    "fields" : {
                        "keyword" : {
                            "type" : "keyword",
                            "ignore_above" : 256
                        }
                    }
                },
                "patentType" : {
                    "type" : "text",
                    "fields" : {
                        "keyword" : {
                            "type" : "keyword",
                            "ignore_above" : 256
                        }
                    }
                },
                "productionDate" : {
                    "properties" : {
                        "iso" : {
                            "type" : "date"
                        },
                        "raw" : {
                            "type" : "text",
                            "fields" : {
                                "keyword" : {
                                    "type" : "keyword",
                                    "ignore_above" : 256
                                }
                            }
                        }
                    }
                },
                "publishedDate" : {
                    "properties" : {
                        "iso" : {
                            "type" : "date"
                        },
                        "raw" : {
                            "type" : "text",
                            "fields" : {
                                "keyword" : {
                                    "type" : "keyword",
                                    "ignore_above" : 256
                                }
                            }
                        }
                    }
                },
                "title" : {
                    "type" : "text",
                    "fields" : {
                        "keyword" : {
                            "type" : "keyword",
                            "ignore_above" : 256
                        }
                    }
                }
            }
        }
    }
    
    if not es.indices.exists(index_name):
        res = es.indices.create(
            index=index_name,
            body=mapping
        )
    

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


    check_file = "inserted2.txt"
    for i,flist in enumerate(splitted_files):
        #解凍
        for f in tqdm(flist):
            # ファイルをオープンする
            test_data = open(check_file, "r")

            # 行ごとにすべて読み込んでリストデータにする
            lines = test_data.readlines()
            #print(lines[0])
            if(str(f)+"\n" in lines):
                continue


            with gzip.open(f,mode = 'rt') as fp:
                lst = [json.loads(s) for s in fp.read().splitlines()]

                for j,l in tqdm(enumerate(lst)):
                    del l["relatedIds"],l["otherIds"],l["agent"],l["applicant"],l["inventors"],l["assignees"],l["examiners"],l["abstract"]["raw"],l["abstract"]["normalized"]
                    del l["description"]["full_raw"],l["description"]["REL_APP_DESC"],l["description"]["DRAWING_DESC"],l["description"]["DETAILED_DESC"]["raw"],l["description"]["DETAILED_DESC"]["normalized"]
                    for k in range(len(l["claims"])):
                        del l["claims"][k]["type"],l["claims"][k]["raw"],l["claims"][k]["normalized"],l["claims"][k]["claimTree"]["parentIds"],l["claims"][k]["claimTree"]["childIds"]
                    del l["citations"],l["classification"]["uspc"],l["classification"]["cpc"]
                    #print(json.dumps(l, indent=2))


                    es.create(index=index_name, id=l['documentId'],body=l)
            
            with open(check_file, mode='a') as fa:
                fa.write(str(f)+"\n")
        print("\nfinished:")
        print(flist)
        #ここまで
    """
    
    #files = list(Path('/Users/higashi/us_data/').glob(r'*.bulk.gz'))

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
    """
