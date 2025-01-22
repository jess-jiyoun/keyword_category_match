from collections import defaultdict
import sys

from pyathena import connect
from pyathena.pandas.util import as_pandas

if len(sys.argv) != 5 :
    sys.exit("Usage: %s keyword_file keyword_click_file keyword_click_nlu_file mapping_file"%sys.argv[0])

keyword_file = sys.argv[1]
#카테고리 분류가 필요한 키워드 리스트
#예시: ../resource/keywords.txt

keyword_click_file = sys.argv[2] 
#키워드 클릭 카테고리 로그데이터
#예시: ../resource/step1/keyword_category_click_20241201_20241231_raw.txt

keyword_nlu_file = sys.argv[3]
#로그데이터 내 키워드의 nlu 결과
#예시: ../resource/step1/keyword_category_click_20241201_20241231_nlu.txt

search2admin_mapping_file = sys.argv[4] 
#mapping 파일, 직접 파일에 입력하거나 주석 처리해 둔 get_search_admin_mapping 함수 실행.
#search_dev.search_categories_admin_categories_mapping에서 가장 최신 버전 사용
#예시: ../resource/mapping_table/search2admin_v1.6.0.txt

#keyword_file = "./testset/query_2025-01-08_2025-01-08.txt" #정답데이터
#keyword_nlu_file = "./1_ctr_result/keyword_nlu_result_20241201_20241231_v2.3.0.txt" #nlu 사전
#keyword_click_file = "./1_ctr_result/keyword_category_click_20241201_20241231.txt" #로그데이터
#search2admin_mapping_file = "./search2admin_mapping_tables/search2admin.1.6.0.txt" #mapping 버전

S3_DIR = 's3://bucketplace-athena-result'
AWS_ACCESS_KEY_ID = 'AKIAVUNY43ERE42JR62G'
AWS_SECRET_ACCESS_KEY = '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'
REGION_NAME = 'ap-northeast-2'
cursor = connect(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=REGION_NAME, s3_staging_dir=S3_DIR).cursor()

unimportant_tag = ["접두사_강조_p", 
                   "접미사_기기_p", "접미사_기능_p", "접미사_범위_p", "접미사_색상_p", "접미사_용도_p", "접미사_형태_p", "접미사_스타일_p",
                   "브랜드_p",
                   "어미_활용형_p",
                   "색상_p", "색상_무채색_p", "색상_조명_p", "색상_파스텔_p",
                   "스타일_p", 
                   "단위_p", "단위_길이_p", "단위_무게_p", "단위_부피_p", 
                   "--_p"]

number_dict = {"일": 1,
               "이": 2,
               "삼": 3,
               "원": 1, 
               "투": 2, 
               "쓰리": 3
               }

def get_search_admin_mapping(search2admin_mapping_file):
    search2admin = defaultdict(list)
    admin2search = defaultdict(list)
    adminid2name = defaultdict(str)

    with open(search2admin_mapping_file, "r", encoding = "utf-8") as f:
        for line in f.readlines():
            line = line.strip()
            superclass, class_, subclass, admin_id, admin_name = line.split("\t")

            search2admin[(superclass, class_, subclass)].append(admin_id)
            admin2search[admin_id].append((superclass, class_, subclass))
            adminid2name[admin_id] = admin_name

    return search2admin, admin2search, adminid2name

"""
def get_search_admin_mapping():
    search2admin = defaultdict(list)
    admin2search = defaultdict(list)
    adminid2name = defaultdict(str)

    query = f
    SELECT *
    FROM
        search_dev.admin2search
    

    cursor.execute(query)
    df = as_pandas(cursor)
    df = df.dropna()

    superclasses = df['superclass']
    classes = df['class']
    admin_ids = df['admin_id']
    admin_full_names = df['admin_full_name']

    for superclass, subclass, admin_id, admin_full_name in zip(superclasses, classes, admin_ids, admin_full_names):
        admin_id = str(admin_id)
        search2admin[(superclass, subclass)].append(admin_id)
        admin2search[admin_id].append((superclass, subclass))
        adminid2name[admin_id] = admin_full_name

    return search2admin, admin2search, adminid2name
"""

def nlu2mappingkey(nlu_result):
    main_class = "" # property에서 제거하기 위해 필요
    key1 = "" # 대분류
    key2 = "" # 소분류
    final_result = []
    pv_result = []

    for word, tag in nlu_result[::-1]:
        if tag[-1] == 'c':
            main_class = (word, tag)
            key1 = tag.split("_")[0]
            key2 = tag.split("_")[-2]
            break
    
    if key1 != "" and key2 != "":
        for word, tag in nlu_result:
            if (word, tag) == main_class: continue

            if tag[-1] == 'c':
                pv_result.append(tag)
                pv_result.append("*")
            else:
                if tag not in unimportant_tag:
                    pv_result.append(tag)
                    if tag == '숫자_p' :
                        if word in number_dict :
                            num_word = number_dict[word]
                        try :
                            num_word = float(word)
                            if num_word > 10:
                                pv_result.append("*")
                            else :
                                pv_result.append(word)
                        except:
                            pv_result.append("*")

                    else:
                        pv_result.append(word)

    final_result = [key1, key2] + pv_result

    if final_result != []:
        return final_result


search2admin, admin2search, adminid2name = get_search_admin_mapping(search2admin_mapping_file)
keyword_nlu_result_dict = defaultdict()

with open(keyword_nlu_file, "r", encoding = "utf-8") as f:
    for line in f.readlines():
        line = line.strip()
        try:
            keyword, _, nlu_result = line.split("\t")
            keyword_nlu_result_dict[keyword] = eval(nlu_result)
        except:
            pass

#nlu_key_search_category_score_dict = defaultdict(float) #2.1
nlu_key_search_category_score_dict = defaultdict(list)

with open(keyword_click_file, "r", encoding = "utf-8") as f:
    for line in f.readlines():
        line = line.strip()
        keyword, qc, click_admin_category_list = line.split("\t")
        click_admin_category_list = eval(click_admin_category_list)

        search_category_list = defaultdict(float)

        for category_score_pair in click_admin_category_list:
            admin_category, score = category_score_pair
            search_categories = admin2search[admin_category]
            for search_category in search_categories:
                search_category_list[search_category] += score

        search_category_list = sorted(search_category_list.items(), key = lambda x : -x[1])
        if keyword not in keyword_nlu_result_dict:
            continue
        nlu_result = keyword_nlu_result_dict[keyword]
        #print(keyword, nlu_result, nlu2mappingkey(nlu_result), search_category_list, sep = "\t")
        
        if nlu2mappingkey(nlu_result) != ['', '']:
            mapping_result = '\t'.join(nlu2mappingkey(nlu_result))
            for category, score in search_category_list:
                #nlu_key_search_category_score_dict[(mapping_result, category)] += score #2.1
                nlu_key_search_category_score_dict[(mapping_result, category)].append(score)
                #print(category, round(score, 3), mapping_result, sep = "\t")

#nlu_key_search_category_score_dict = dict(sorted(nlu_key_search_category_score_dict.items(), key = lambda x : -x[1])) #2.1

for nlu_key, category in nlu_key_search_category_score_dict:
    #score = round(nlu_key_search_category_score_dict[(nlu_key, category)], 3) #2.1
    score = round(max(nlu_key_search_category_score_dict[(nlu_key, category)]), 3) #2.2
    print(category, score, nlu_key, sep = "\t") #2.2

    #if score > 1 : #2.2
        #print(category, score, nlu_key, sep = "\t") #2.2