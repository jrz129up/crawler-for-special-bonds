import requests
import pandas as pd
import time
import os
import random

output_folder = 'your path'
os.makedirs(output_folder, exist_ok=True)
all_data_frames = []
url = 'the url of special bonds that will be scraped(actually in this context, it is not a url'

headers = {
    hidden
    pay attention to the formate
}

post_data ={
    for-example(can be found in the payload)
    "regions": [],
    "city": None,
    "county": None,
    "projectName": None,
    "projectType2": None,
}

base_headers = {
    hidden
    pay attention to the formate
}

post_columns = [
    'projectBatchCount', 'bondId', 'bondName', 'cityName', 'countyName',
    'issueDate', 'issueDateAll', 'issueInterestRate', 'issueInterestRateMin',
    'issueTerm', 'presentAsSpecialAmount', 'presentIssueAmount',
    'presentIssueAmountAll', 'projectBatchId', 'projectBatchName', 'projectId',
    'projectName', 'projectType3Name', 'regionName', 'termNumMin', 'totalAmount',
    'totalInvestment'
]

get_columns1 = [
    'capital', 'expectedReturn', 'projectCost', 'sourceIncome', 'constructionContent', 'startDate', 'endDate',
    'operationStartDate', 'operationEndDate']

get_columns2= ['batchNum', 'bondId', 'bondName', 'firstPublishDate', 'issueInterestRate']

file_template = 'page_data_{page}.csv'
max_list_items = 0
all_column_names = set(post_columns + get_columns1)
all_rows_data = []
for page_num in range(1, 1989):
    collected_data = []
    for attempt in range(5):
        try:
            response = requests.post(url, headers=headers, json=post_data, params={'pageNum': page_num, 'pageSize': 50, 'isAsc': 'desc', 'orderByColumn': 'issueDate'}, timeout=30)
            if response.status_code == 200:
                data_json = response.json()
                collected_data.extend(data_json['rows'])
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
        time.sleep(random.uniform(0.5, 2))

    df_collected = pd.DataFrame(collected_data)
    df_collected.drop_duplicates(subset='projectId', inplace=True)
    combined_data=[]
    for _, row in df_collected.iterrows():
        basic_info = {key: row.get(key, None) for key in post_columns}
        project_batch_id = row['projectBatchId']
        bond_id = row['bondId']
        some hidden code

        response1 = requests.get(url1, headers=headers1)
        response1.raise_for_status()
        data1 = response1.json()
        data1_extracted = {key: data1['data'].get(key, None) for key in get_columns1}

        some hidden code
        response2 = requests.get(url2, headers=headers1)
        response2.raise_for_status()
        data2 = response2.json()

        combined_row_data = basic_info.copy()
        combined_row_data.update(data1_extracted)

        if 'list' in data2['data'] and data2['data']['list']:
            list_items = data2['data']['list']
            max_list_items = max(max_list_items, len(list_items))

        some hidden but essential loops for names uesd for the compatibility
                    
        combined_data.append(combined_row_data)
    df_combined = pd.DataFrame(combined_data)
    page_file = os.path.join('path', f'name')
    df_combined.to_csv(page_file, index=False, encoding='utf_8_sig')
    print(f'Processed data for page {page_num}')

output_folder1 = 'path'
os.makedirs(output_folder, exist_ok=True)
csv_files = [os.path.join(output_folder1, f) for f in os.listdir(output_folder1) if f.startswith('page_data_')]
df_alll = pd.concat((pd.read_csv(f) for f in csv_files)).drop_duplicates(subset='projectId')
combined_file = os.path.join(output_folder, 'final_combined_data.csv')
df_alll.to_csv(combined_file, index=False, encoding='utf_8_sig')

print(f'done, {combined_file}')
