"""
jsonファイルのうち全部をelasticsearchに挿入する必要はないので
必要な部分を抜き出してinsertする
"""

import json
from pathlib import Path

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConflictError
from elasticsearch.exceptions import ConnectionTimeout
from tqdm import tqdm
import xmljson
from lxml.etree import parse
import pickle

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

def allkeys(a,a_key=""):
    if isinstance(a,list):
        for item in a:
            yield from allkeys(item,a_key)
    elif isinstance(a, str):
        yield (a,a_key)
    else:
        for key, value in a.items():
            yield from allkeys(value,key)

if __name__ == '__main__':    
    """
    #初めのみ
    files = list(Path('/Users/higashi/Downloads/EP/').glob(r'**/*.xml'))
    """
    
    #files = list(Path('/Users/higashi/Downloads/EP/000000/33/11/').glob(r'**/*.xml'))
    check_file = "inserted.txt"
    es = Elasticsearch("http://localhost:9200")
    index_name = 'clef_patent'
    #es.indices.delete(index=index_name)
    #es.indices.create(index=index_name)

    with open("file_list.pkl","rb")as f:
        files = pickle.load(f)
    
    mapping = {
        "mappings" : {
            "properties" : {
                "ucid" : {
                    "type" : "text",
                    "fields" : {
                        "keyword" : {
                            "type" : "keyword",
                            "ignore_above" : 256
                        }
                    }
                },
                "lang" : {
                    "type" : "text",
                    "fields" : {
                        "keyword" : {
                            "type" : "keyword",
                            "ignore_above" : 256
                        }
                    }
                },
                "abstract" : {
                    "properties" : {
                        "lang" : {
                            "type" : "text",
                            "fields" : {
                                "keyword" : {
                                    "type" : "keyword",
                                    "ignore_above" : 256
                                }
                            }
                        },
                        "p" : {
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
                "description" : {
                    "properties" : {
                        "lang" : {
                            "type" : "text",
                            "fields" : {
                                "keyword" : {
                                    "type" : "keyword",
                                    "ignore_above" : 256
                                }
                            }
                        },
                        "p" : {
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
                "claims": {
                    "properties" : {
                        "lang" : {
                            "type" : "text",
                            "fields" : {
                                "keyword" : {
                                    "type" : "keyword",
                                    "ignore_above" : 256
                                }
                            }
                        },
                        "claim" : {
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
        }   
    }

   
    if not es.indices.exists(index_name):
        res = es.indices.create(
            index=index_name,
            body=mapping
        )
    
    print("start")
    for i,f in tqdm(enumerate(files[300500 :])):

        print(f)
        _body = {
            "ucid" : { },
            "lang" : { },
            "abstract" : {
                "lang" : { },
                "p" : { }
            }, 
            "description" : {
                "lang" : { },
                "p" : { }
            },
            "claims" :{
                "lang" : { },
                "claim": { }
            } 
        }
        tmp_body = _body
        tree = parse(str(f))
        # すべてのタグの取得
        root = tree.getroot()
        # JSONファイルへの書き込み
        data = xmljson.yahoo.data(root)
        tmp_body["ucid"] = data["patent-document"].get("ucid")
        if(data["patent-document"].get("lang")!="EN"):
            #英語のみ
            continue
        tmp_body["lang"] = data["patent-document"].get("lang")

        if data["patent-document"].get("abstract",False):
            """
            abstractの中の構造
            "abstract" : { "lang":{},"p":{}}の時と
            "abstract" : [{ "lang":{},"p":{}},{ "lang":{},"p":{}}]のように複数言語で書いてある時がある
            """
            #print(type(data["patent-document"]["abstract"]))
            if(type(data["patent-document"]["abstract"])is list):
                #言語が複数の時はENだけ
                for abst in data["patent-document"]["abstract"]:
                    if abst["lang"]=="EN":
                        #英語のみ挿入
                        tmp_body["abstract"]["lang"] = abst["lang"]
                        tmp = abst["p"]
                        tmp_text = ''
                        for i,t in enumerate(tmp):
                            if (type(t)is str):
                                tmp_text += t.replace( '\n' , '' )
                        tmp_body["abstract"]["p"] = tmp_text
                    else:
                        pass
            else:
                # 言語が一つの時
                if data["patent-document"]["abstract"].get("lang",False):
                    tmp_body["abstract"]["lang"] = data["patent-document"]["abstract"]["lang"]
                else:
                    del tmp_body["abstract"]["lang"]
                #descripationを複雑な形から簡易な形へ
                if data["patent-document"]["abstract"].get("p",False):
                    tmp = data["patent-document"]["abstract"]["p"]
                    tmp_text = ''
                    for i,t in enumerate(tmp):
                        if (type(t)is str):
                            tmp_text += t.replace( '\n' , '' )
                    tmp_body["abstract"]["p"] = tmp_text
                else:
                    del tmp_body["abstract"]["p"]

        else:
            del tmp_body["abstract"]

        if data["patent-document"].get("description",False):
            if data["patent-document"]["description"].get("lang",False):
                tmp_body["description"]["lang"] = data["patent-document"]["description"].get("lang")
            #descripationを複雑な形から簡易な形へ
            if data["patent-document"]["description"].get("p",False):
                tmp = data["patent-document"]["description"].get("p")
                tmp_text = ''
                for i,t in enumerate(tmp):
                    if (type(t)is str):
                        tmp_text += t.replace( '\n' , '' )
                tmp_body["description"]["p"] = tmp_text
            else:
                del tmp_body["description"]["p"]
        else:
            del tmp_body["description"]
        

        if data["patent-document"].get("claims",False):
            """
            claimsの中の構造
            複数クレームがある時と、複数言語がある時がある
            """
            text = ""
            if(type(data["patent-document"]["claims"])is list):
                #言語が複数の時はENだけ
                for claims in data["patent-document"]["claims"]:
                    if claims["lang"]=="EN":
                        #英語のみ挿入
                        tmp_body["claims"]["lang"] = claims["lang"]
                        for value in list(allkeys(claims["claim"])):
                            if(value[1]=="content" or value[1]=="claim-text"):
                                text += value[0].replace( '\n' , '' )
                        """
                        if (type(claims["claim"])is list):
                            #claimが複数あるとき
                            for claim in claims["claim"]:
                                print(json.dumps(claim,indent=2))
                                if(type(claim["claim-text"])is str):
                                    #claim-textがテキストだけの時
                                    text += claim["claim-text"].replace( '\n' , '' )
                                else:
                                    #claim-textがdictを含む時
                                    text += (claim["claim-text"].get("content","")).replace( '\n' , '' )
                                    for text_list in claim["claim-text"]["claim-text"]["claim-text"]:
                                        text += text_list.replace( '\n' , '' )
                        else:
                            if(type(claims["claim"]["claim-text"])is str):
                                #claim-textがテキストだけの時
                                text += claims["claim"]["claim-text"].replace( '\n' , '' )
                            else:
                                #claim-textがdictを含む時
                                text += (claims["claim"]["claim-text"].get("content","")).replace( '\n' , '' )
                                for text_list in claims["claim"]["claim-text"]["claim-text"]["claim-text"]:
                                    text += text_list.replace( '\n' , '' )
                        """
                        tmp_body["claims"]["claim"] = text
                        
                    else:
                        pass
            else:
                #言語が一つの時
                claims = data["patent-document"]["claims"]
                tmp_body["claims"]["lang"] = claims["lang"]
                for value in list(allkeys(claims)):
                    if(value[1]=="content" or value[1]=="claim-text"):
                        text += value[0].replace( '\n' , '' )
                """
                if (type(claims["claim"])is list):
                    #claimが複数あるとき
                    for claim in claims["claim"]:
                        if(type(claim["claim-text"])is str):
                            #claim-textがテキストだけの時
                            text += claim["claim-text"].replace( '\n' , '' )
                        else:
                            #claim-textがdictを含む時
                            text += (claim["claim-text"].get("content","")).replace( '\n' , '' )
                            for text_list in claim["claim-text"]["claim-text"]["claim-text"]:
                                text += text_list.replace( '\n' , '' )
                else:
                    if(type(claims["claim"]["claim-text"])is str):
                        #claim-textがテキストだけの時
                        text += claims["claim"]["claim-text"].replace( '\n' , '' )
                    else:
                        #claim-textがdictを含む時
                        text += (claims["claim"]["claim-text"].get("content","")).replace( '\n' , '' )
                        for text_list in claims["claim"]["claim-text"]["claim-text"]["claim-text"]:
                            text += text_list.replace( '\n' , '' )
                """
                tmp_body["claims"]["claim"] = text
        else:
            del tmp_body["claims"]

    

        #print(json.dumps(tmp_body,indent=2))

        try:
            es.create(index=index_name, id=tmp_body["ucid"],body=json.dumps(tmp_body,indent=2) )
            with open(check_file, mode='a') as fa:
                fa.write(str(f)+"\n")
        except ConflictError:
            with open(check_file, "r") as test_data:
                # 行ごとにすべて読み込んでリストデータにする
                lines = test_data.readlines()
                print("error")
                if(str(f)+"\n" in lines):
                    continue
            with open("pass_conflict.txt", mode='a') as fa:
                fa.write(str(f)+"\n")
        except ConnectionTimeout:
            with open("pass_timeout.txt", mode='a') as fa:
                fa.write(str(f)+"\n")
