import argparse
from collections import defaultdict

from pyathena import connect
from pyathena.pandas.util import as_pandas

cursor = connect(s3_staging_dir='s3://bucketplace-athena-result').cursor()

def get_keyword_query(start_date, end_date) :

    query = f"""
WITH log AS (
SELECT
    *
FROM
(
SELECT
    search_keyword,
    object_id,
    COUNT(IF(category = 'IMPRESSION', 1, NULL)) AS impression,
    COUNT(IF(category = 'CLICK', 1, NULL)) AS click,
    CAST(COUNT(IF(category = 'CLICK', 1, NULL)) AS DOUBLE) / COUNT(IF(category = 'IMPRESSION', 1, NULL)) AS ctr
FROM
    log.analyst_log_table AS l
WHERE
    l.date BETWEEN '{start_date}' AND '{end_date}'
    AND l.url_path IN ('https://ohou.se/search/index', 'https://ohou.se/search/integrated', 'https://ohou.se/productions/feed')
    AND l.object_type = 'PRODUCTION'
    AND l.category IN ('IMPRESSION', 'CLICK')
GROUP BY
    1, 2
)
WHERE
    impression >= 100
    AND ctr > 0.01
)
SELECT
    search_keyword,
    admin_category_id,
    full_display_name,
    COUNT(*) AS product_cnt,
    SUM(impression) AS impression,
    SUM(click) AS click,
    CAST(SUM(click) AS DOUBLE) / SUM(impression) AS ctr
FROM
(
    SELECT
        l.*,
        p.admin_category_id,
        ac.full_display_name
    FROM
        log AS l
    LEFT JOIN
        dump.productions AS p ON l.object_id = CAST(p.id AS VARCHAR)
    LEFT JOIN
        dump.admin_categories AS ac ON ac.id = p.admin_category_id
)
GROUP BY
    1, 2, 3
ORDER BY
    1, 4 DESC, 7 DESC
    """
    cursor.execute(query)
    df = as_pandas(cursor)
    df = df.dropna()

    return df

def get_keyword_category_clicks(df):

    keyword_total_click = defaultdict(int)
    keyword_category_click = defaultdict(int)
    keyword_category_click_rate = defaultdict(list)

    search_keywords = df['search_keyword']
    admin_category_ids = df['admin_category_id']
    clicks = df['click']

    for keyword, category, click in zip(search_keywords, admin_category_ids, clicks):
        keyword = keyword.strip().replace(" ", "")
        category = str(int(category))
        click = int(click)

        keyword_total_click[keyword] += click
        keyword_category_click[(keyword, category)] += click

    
    for keyword_category in keyword_category_click:
        keyword, category = keyword_category
        category_click = keyword_category_click[keyword_category]
        total_click = keyword_total_click[keyword]

        category_click_rate = float(round(category_click / (total_click + 100), 3))
        if category_click_rate > 0:
            keyword_category_click_rate[keyword].append((category, category_click_rate))
    

    return keyword_total_click, keyword_category_click_rate

def main(start_date, end_date, output_file):
    df = get_keyword_query(start_date, end_date)
    keyword_total_click, keyword_category_click_rate = get_keyword_category_clicks(df)

    keyword_total_click = dict(sorted(keyword_total_click.items(), key = lambda x : -x[1]))

    with open(output_file, "w", encoding = "utf-8") as f:
        print("keyword", "qc", "category_list", sep = "\t", file = f)
        for keyword in keyword_total_click:
            total_click = keyword_total_click[keyword]
            category_click_rate = keyword_category_click_rate[keyword]
            print(keyword, total_click, category_click_rate, sep = "\t", file = f)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', required=True, help="yyyy-mm-dd")
    parser.add_argument('--end_date', required=True, help="yyyy-mm-dd")
    parser.add_argument('--output')
    args = parser.parse_args()

    start_date = args.start_date
    end_date = args.end_date
    output_file = args.output

    main(start_date, end_date, output_file)