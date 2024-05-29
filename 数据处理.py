import pandas as pd
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
import jionlp as jio
import os
import imageio
from matplotlib.colors import LinearSegmentedColormap
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False
excel_path = 'path'
china_shapefile_path = 'shp'

#1.import
df = pd.read_csv('data', low_memory=False)

#2.classify
keywords = [distinct words]

def contains_keywords(row):
    return any(keyword in str(row['projectType3Name']) for keyword in keywords) or \
           any(keyword in str(row['projectName']) for keyword in keywords)

filtered_df = df[df.apply(contains_keywords, axis=1)]

#3.
retain_columns = [
    'projectId', 'projectBatchName', 'projectName', 'projectBatchCount', 'projectBatchId',
    'projectType3Name', 'constructionContent', 'cityName', 'countyName',
    'capital', 'expectedReturn', 'sourceIncome', 'projectCost', 'startDate', 'endDate',
    'operationStartDate', 'operationEndDate', 'totalAmount', 'totalInvestment',
    'presentAsSpecialAmount', 'presentIssueAmount', 'presentIssueAmountAll'
]

new_rows = []

for _, row in filtered_df.iterrows():
    add_new_row = True

    if pd.isna(row['presentIssueAmountAll']) or row['presentIssueAmountAll'] == '':
        add_new_row = False
    else:
        amounts_all = str(row['presentIssueAmountAll']).split(',')

        if len(amounts_all) != row['projectBatchCount']:
            add_new_row = False

    if add_new_row:
        for i in range(1, row['projectBatchCount'] + 1):
            new_row = {col: row[col] for col in retain_columns if col in row}
            for col in ['bondId', 'bondName', 'firstPublishDate', 'issueInterestRate']:
                new_row[col] = row.get(f'{col}{i}', None)
            new_row['bondInvest'] = amounts_all[i-1]
            new_rows.append(new_row)

new_df = pd.DataFrame(new_rows)

#4.descriptive statistics
numeric_columns = new_df.select_dtypes(include=[np.number]).columns.tolist()

def generate_descriptive_stats_md(df, numeric_cols, filepath):
    descriptive_stats = df[numeric_cols].describe()
    markdown_text = descriptive_stats.to_markdown()
    with open(filepath, 'w') as f:
        f.write(markdown_text)
    return filepath

def generate_counts_md(df, column, filepath):
    counts = df[column].value_counts()
    percentages = df[column].value_counts(normalize=True) * 100
    counts_df = pd.DataFrame({'Counts': counts, 'Percentage': percentages.round(2)})
    total_counts = counts.sum()
    total_percentage = percentages.sum()
    total_row = pd.DataFrame({'Counts': [total_counts], 'Percentage': [total_percentage.round(2)]}, index=['全部'])
    counts_df = pd.concat([counts_df, total_row])
    markdown_text = counts_df.to_markdown()
    with open(filepath, 'w') as f:
        f.write(markdown_text)
    return filepath

descriptive_stats_md_path = generate_descriptive_stats_md(new_df, numeric_columns, 'md')
project_type_counts_md_path = generate_counts_md(new_df, 'projectType3Name', 'md')
city_name_counts_md_path = generate_counts_md(new_df, 'cityName', 'md')

#5.excel
new_df['firstPublishDate'] = pd.to_datetime(new_df['firstPublishDate'], errors='coerce')
new_df['YearMonth'] = new_df['firstPublishDate'].dt.to_period('M').astype(str)

with pd.ExcelWriter('xlsx') as writer:
    for name, group in new_df.groupby('YearMonth'):
        group.to_excel(writer, sheet_name=name, index=False)

#6.figures
def parse_province(city_name,bond_name):
    if pd.isna(city_name) or city_name.strip() == '' or jio.parse_location(city_name).get('province') == None :
        location_info = jio.parse_location(bond_name)
        return location_info.get('province')
    else:
        location_info = jio.parse_location(city_name)
        return location_info.get('province')

def check_province_in_full(shp_name, full_name):
    return shp_name in full_name

gdf_provinces = gpd.read_file(china_shapefile_path)
xls = pd.ExcelFile(excel_path)

#6.1每个月的图
for sheet_name in xls.sheet_names:
    df_sheet = pd.read_excel(xls, sheet_name=sheet_name)
    gdf_provinces['bondInvest'] = 0
    if not df_sheet.empty:
        df_sheet['province'] = df_sheet.apply(lambda row: parse_province(row['cityName'], row['bondName']), axis=1)
        df_sheet['mapped_province'] = ''
        for index, row in df_sheet.iterrows():
            for shp_name in gdf_provinces['NAME']:
                if check_province_in_full(shp_name, row['province']):
                    df_sheet.at[index, 'mapped_province'] = shp_name
                    break
        province_invest = df_sheet.groupby('mapped_province')['bondInvest'].sum().to_dict()
        gdf_provinces['bondInvest'] = gdf_provinces['NAME'].map(province_invest).fillna(0)
    else:
        province_invest = {}

    gdf_provinces['center'] = gdf_provinces.geometry.centroid
    gdf_provinces_points = gdf_provinces.copy()
    gdf_provinces_points.set_geometry("center", inplace=True)
    light_green_to_dark = LinearSegmentedColormap.from_list(
        "CustomGreen", ["#a0f0a0", "#006400"])

    offsets = {
        '河北': (-10000, -100000),
        '天津': (15000, -10000),
    }
    fig, ax = plt.subplots(1, 1, figsize=(10, 10))
    gdf_provinces.plot(column='bondInvest', ax=ax, legend=True, cmap=light_green_to_dark, vmin=0, vmax=1e7)
    for idx, row in gdf_provinces.iterrows():
        if row['bondInvest'] > 0:
            offset = offsets.get(row['NAME'], (0, 0))
            x = row['center'].x + offset[0]
            y = row['center'].y + offset[1]
            plt.text(s=f"{row['bondInvest']/10000000:.3f}",
                     x=x, y=y,
                     horizontalalignment='center', verticalalignment='center',
                     fontsize=6)
    plt.title(f'{sheet_name} Map')

    output_path = os.path.join('E:/人大农发硕士/债券/photomonth/', f"{sheet_name}_map.png")
    plt.savefig(output_path)
    plt.close()
    print(f"{sheet_name}_map.png已保存")
print("所有地图已成功保存为图片文件！")

#6.2累积图
province_invest_cumulative = dict.fromkeys(gdf_provinces['NAME'], 0)
last_valid_data = None
for sheet_name in xls.sheet_names:
    df_sheet = pd.read_excel(xls, sheet_name=sheet_name)
    if not df_sheet.empty:
        df_sheet['province'] = df_sheet.apply(lambda row: parse_province(row['cityName'], row['bondName']), axis=1)

        df_sheet['mapped_province'] = ''
        for index, row in df_sheet.iterrows():
            for shp_name in gdf_provinces['NAME']:
                if check_province_in_full(shp_name, row['province']):
                    df_sheet.at[index, 'mapped_province'] = shp_name
                    break

        for index, row in df_sheet.iterrows():
            province = row['mapped_province']
            invest = row['bondInvest']
            if province:
                province_invest_cumulative[province] += invest
        last_valid_data = province_invest_cumulative.copy()
    else:
        province_invest_cumulative = last_valid_data


    gdf_provinces['bondInvest'] = gdf_provinces['NAME'].map(province_invest_cumulative).fillna(0)
    gdf_provinces['center'] = gdf_provinces.geometry.centroid
    gdf_provinces_points = gdf_provinces.copy()
    gdf_provinces_points.set_geometry("center", inplace=True)
    light_green_to_dark = LinearSegmentedColormap.from_list(
        "CustomGreen", ["#a0f0a0", "#006400"])

    offsets = {
        '河北': (-10000, -100000),
        '天津': (15000, -10000),
    }
    fig, ax = plt.subplots(1, 1, figsize=(10, 10))
    gdf_provinces.plot(column='bondInvest', ax=ax, legend=True, cmap=light_green_to_dark, vmin=0, vmax=1.7e7)
    for idx, row in gdf_provinces.iterrows():
        if row['bondInvest'] > 0:
            offset = offsets.get(row['NAME'], (0, 0))
            x = row['center'].x + offset[0]
            y = row['center'].y + offset[1]
            plt.text(s=f"{row['bondInvest']/10000000:.3f}",
                     x=x, y=y,
                     horizontalalignment='center', verticalalignment='center',
                     fontsize=6)
    plt.title(f'Cumulative Investment Map - Up to {sheet_name}')

    output_path = os.path.join('E:/人大农发硕士/债券/photobymonth/', f"Cumulative_{sheet_name}_map.png")
    plt.savefig(output_path)
    plt.close()
    print(sheet_name)

print("所有累积地图已成功保存为图片文件！")

#6.3合成动图
images_folder = 'E:/人大农发硕士/债券/photobymonth/'
images_files = sorted(
    [img for img in os.listdir(images_folder) if img.endswith(".png")]
)
with imageio.get_writer('E:/人大农发硕士/债券/photobymonth/mygif.gif', mode='I', duration=100) as writer:
    for filename in images_files:
        image_path = os.path.join(images_folder, filename)
        image = imageio.v3.imread(image_path)
        writer.append_data(image)
print("GIF动图已创建完成！")
