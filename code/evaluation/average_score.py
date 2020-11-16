"""
prelファイルからscoreの平均を算出
"""

if __name__ == '__main__':   

    with open('../../result/TFiDF_normal_cleftext_fix.prel', mode='r') as f:
        # 行ごとにすべて読み込んでリストデータにする
        lines = f.readlines()
        doc_avescore_list = []  #ドキュメントごとに平均スコアを格納
        prev_docid = ""         
        doc_totalscore = 0      #ドキュメントごとのトータルスコア
        total_score = 0         #トータルスコア

        cnt = 0
        for line in lines:
            tmp = line.split(' ')
            if prev_docid=="":
                prev_docid = tmp[0]
                cnt+=1
                doc_totalscore += float(tmp[4])
            elif prev_docid==tmp[0]:
                cnt+=1
                doc_totalscore += float(tmp[4])

            else:
                doc_avescore_list.append((prev_docid,doc_totalscore/cnt))
                total_score += doc_totalscore/cnt
                prev_docid = tmp[0]
                doc_totalscore = 0
                cnt = 0
        else:
            doc_avescore_list.append((prev_docid,doc_totalscore/cnt))
            total_score += doc_totalscore/cnt

        print(doc_avescore_list )
        print(total_score/len(doc_avescore_list))
            
        