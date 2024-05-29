import requests
import pandas as pd
import time
import os
import random

output_folder = 'E:/人大农发硕士/债券/'
os.makedirs(output_folder, exist_ok=True)
all_data_frames = []
# 请求的URL
url = 'https://www.zhuanxiangzhaiquan.com/dev-api/project/homePage/searchProjectES/'

# 请求头部信息
headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
    'Connection': 'keep-alive',
    'Content-Type': 'application/json',
    'Host': 'www.zhuanxiangzhaiquan.com',
    'Origin': 'https://www.zhuanxiangzhaiquan.com',
    'Referer': 'https://www.zhuanxiangzhaiquan.com/projectInfomation',
    'Sec-Ch-Ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
}

post_data ={
    "regions": [],
    "city": None,
    "county": None,
    "projectName": None,
    "projectType2": None,
    "startTotalInvestment": None,
    "endTotalInvestment": None,
    "bondName": None,
    "endIssueInterestRate": None,
    "startIssueInterestRate": None,
    "isPortfolioFinancing": None,
    "isSpecialDebtAsCapital": None,
    "isEarlyRepayPrincipal": None,
    "allIssueTerm": None,
    "powerIssueTerm": None,
    "smallBankDevelopment": None,
    "beginTime": None,
    "endTime": None,
    "region": None,
    "projectType3": None,
    "issueYears": None,
    "queryMode": "exact",
}

base_headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
    'Connection': 'keep-alive',
    'Host': 'www.zhuanxiangzhaiquan.com',
    'Sec-Ch-Ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
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
for page_num in range(704, 1989):
    collected_data = []
    for attempt in range(5):  # 每页进行5次尝试
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
        headers1 = base_headers.copy()
        headers1['Referer'] = f'https://www.zhuanxiangzhaiquan.com/projectInfomationDetails?projectBatchId={project_batch_id}&bondId={bond_id}'

        url1 = f'https://www.zhuanxiangzhaiquan.com/dev-api/project/debtProjectBatch/projectInfo/{project_batch_id}'
        response1 = requests.get(url1, headers=headers1)
        response1.raise_for_status()
        data1 = response1.json()
        data1_extracted = {key: data1['data'].get(key, None) for key in get_columns1}

        url2 = f'https://www.zhuanxiangzhaiquan.com/dev-api/project/debtProjectBatch/allBatch/{project_batch_id}'
        response2 = requests.get(url2, headers=headers1)
        response2.raise_for_status()
        data2 = response2.json()

        combined_row_data = basic_info.copy()
        combined_row_data.update(data1_extracted)

        if 'list' in data2['data'] and data2['data']['list']:
            list_items = data2['data']['list']
            max_list_items = max(max_list_items, len(list_items))  # 更新最大list_items数

            # 为list_items中的每个元素创建带后缀的列名
            for i, item in enumerate(list_items):
                for key in get_columns2:
                    new_key = f'{key}{i + 1}'
                    combined_row_data[new_key] = item[key]
                    all_column_names.add(new_key)
        combined_data.append(combined_row_data)
    df_combined = pd.DataFrame(combined_data)
    page_file = os.path.join('E:/人大农发硕士/债券/subfiles', f'page_data_{page_num}.csv')
    df_combined.to_csv(page_file, index=False, encoding='utf_8_sig')
    print(f'Processed data for page {page_num}')

output_folder1 = 'E:/人大农发硕士/债券/subfiles'
os.makedirs(output_folder, exist_ok=True)
csv_files = [os.path.join(output_folder1, f) for f in os.listdir(output_folder1) if f.startswith('page_data_')]
df_alll = pd.concat((pd.read_csv(f) for f in csv_files)).drop_duplicates(subset='projectId')
combined_file = os.path.join(output_folder, 'final_combined_data.csv')
df_alll.to_csv(combined_file, index=False, encoding='utf_8_sig')

print(f'所有页面数据已合并到: {combined_file}')