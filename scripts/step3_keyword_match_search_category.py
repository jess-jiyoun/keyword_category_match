from collections import defaultdict
import common
import sys

if len(sys.argv) != 3 :
    sys.exit("Usage: %s mapping_file keyword_nlu_file"%sys.argv[0])


mapping_file = sys.argv[1]
# step2에서 만든 mapping file 활용
# 예시: ../resource/step2/mapping_table_v1.6.0_nlu_2.3.0.txt

keyword_nlu_file = sys.argv[2]
#카테고리 매칭이 필요한 키워드의 nlu 결과
# 예시: ../resource/keywords.nlu.txt


#mapping_file = "./2_mapping_table/mapping_table.v1.6.0_nlu_v2.3.0.txt"
#keyword_nlu_file = "./testset/query_2025-01-08_2025-01-08.nlu.txt"
unimportant_tag = common.get_unimportant_tag()

def reading_mapping_table(mapping_file):

    with open(mapping_file, "r", encoding ="utf-8") as f :
        key2category = defaultdict(set)
        category2prop = defaultdict(set)

        for line in f.readlines():
            line = line.strip()
            infos = line.split("\t")

            category = infos[0]
            score = infos[1]
            key = tuple(infos[2:4])
            properties = infos[4:]

            key2category[key].add(category)
            
            key_properties = []

            for i in range(0, len(properties), 2):
                tag = properties[i]
                value = properties[i+1]
                key_properties.append((tag, value))
            

            key_properties = tuple(sorted(key_properties, key = lambda x : x[0]))
            category2prop[category].add((score, key, key_properties))
    
    return key2category, category2prop

def make_tag_value(nlu_result):
    # nlu_result: [('전신', '사이즈_길이_p'), ('거울', '가구_거울_c')]
    tags = []
    tag_values = []

    for token in nlu_result:
        value, tag = token
        tags.append(tag)
        tag_values.append((tag, value))
    
    return tags, tag_values

def make_query_keys(nlu_result, query_tag, query_tag_value):
    # nlu_result: [('전신', '사이즈_길이_p'), ('거울', '가구_거울_c')]

    for token in nlu_result[::-1]:
        value, tag = token
        if tag.endswith("_c"):
            key1 = tag.split("_")[0]
            key2 = tag.split("_")[-2]

            query_tag.remove(tag) # key로 사용된 태그는 삭제
            query_tag_value.remove((tag, value)) # key로 사용된 태그는 삭제
            break

    
    return (key1, key2), query_tag, query_tag_value    

def property_filter(query_tag, query_tag_value, query_key, score_key_properties):
    score, mapping_key, properties = score_key_properties

    if query_key != mapping_key:
        return False
    
    for property in properties:
        tag, value = property
        if tag[-1] == 'p':
            if tag not in unimportant_tag and tag not in query_tag:
                return False
            elif property not in query_tag_value:
                return False
        
        elif tag[-1] == 'c':
            if tag not in query_tag:
                return False
    
    return True

def get_property_score(query_tag_value_revised, mapping_property):

    query_tag_length = len(query_tag_value_revised)
    query_mapping_property_length = len(mapping_property)

    if query_tag_length == query_mapping_property_length:
        property_score = 1

    if query_tag_length != query_mapping_property_length: 
        property_score = 1 / (max(query_tag_length, query_mapping_property_length)+1)
        property_score = property_score ** 2

    return round(property_score, 3)

def get_category_score(query, query_key, candidate):

    if query_key == eval(candidate):
        category_score = 1
    else:
        candidate_superclass, candidate_class, candidate_subclass = eval(candidate)
        common_character = set(list(query)) & set(list(candidate_class))
        category_score = len(common_character) / len(max(set(list(query)), set(list(candidate_class))))
    

    return category_score


key2category, category2prop = reading_mapping_table(mapping_file)


with open(keyword_nlu_file, "r", encoding = "utf-8") as f:
    for line in f.readlines():
        line = line.strip()
        max_score = -1

        try:
            keyword, _, nlu_result = line.split("\t")
            nlu_result = eval(nlu_result)
            query_tag, query_tag_value = make_tag_value(nlu_result)
            query_key, query_tag_revised, query_tag_value_revised = make_query_keys(nlu_result, query_tag, query_tag_value)
            
            candidate_category = key2category[query_key]
            max_candidate = ""
            max_mapping_key = ""
            max_mapping_property = ""

            for candidate in candidate_category:
                properties = category2prop[candidate]

                for property in properties :
                    if property_filter(query_tag_revised, query_tag_value_revised, query_key, property):
                        click_score, mapping_key, mapping_property = property

                        click_score = float(click_score)
                        property_score = get_property_score(query_tag_value_revised, mapping_property)
                        category_score = get_category_score(keyword, query_key, candidate)
                        
                        score = round(click_score + property_score + category_score, 3)
                        #print(keyword, score, click_score, property_score, category_score, candidate, property, sep = "\t")

                        if score > max_score :
                            max_score = score
                            max_candidate = candidate
                            max_mapping_key = mapping_key
                            max_mapping_property = mapping_property

            print(keyword, max_score, max_candidate, max_mapping_key, max_mapping_property, sep = "\t")
        except:
            print(line)

