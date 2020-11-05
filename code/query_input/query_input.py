'''
input: param:clefかntcir 
output: querydocument_list,docid_list,title_list
'''
import sys
import json
import os


def get_querydata(param):
    file = '../dataset/DATA/'+param+'.query.bulk'
    with open(file, 'r') as f:
        querydocument_list = []
        docid_list = []
        title_list = []

        for line in f:
            df = json.loads(line)
            querydocument_list.append(df['text'])
            docid_list.append(df['docid'])
            title_list.append(df['title'])

    return querydocument_list, docid_list, title_list

