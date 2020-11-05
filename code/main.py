from query_input.query_input import get_querydata

if __name__ == '__main__':
    querydocument_list, docid_list, title_list = get_querydata('clef')
    print(querydocument_list[0])
    print(docid_list[0])
    print(title_list[0])