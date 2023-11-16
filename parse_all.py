import pandas as pd
import requests
import csv
from lxml import html
import os
import threading
import logging
from concurrent.futures import ThreadPoolExecutor

# 创建一个logger
logger = logging.getLogger('location_parser')
logger.setLevel(logging.INFO)

# 创建一个handler，用于写入日志文件
file_handler = logging.FileHandler('location_parser.log', mode='a')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# 再创建一个handler，用于将日志输出到控制台
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# 将handlers添加到logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

lock = threading.Lock()

def find_location_columns(df):
    # 自动检测经纬度列
    longitude_col = latitude_col = None
    for col in df.columns:
        col_lower = col.lower()
        if 'longitude' in col_lower or '经度' in col_lower:
            longitude_col = col
        elif 'latitude' in col_lower or '纬度' in col_lower:
            latitude_col = col

    if longitude_col is None or latitude_col is None:
        raise
        
    return longitude_col, latitude_col

api_key_list = ["key1", "key2", "key3"]
api_key_now_idx = 0
api_key = api_key_list[api_key_now_idx]
longitude_col = None
latitude_col = None
results = []

def parseLocation(file_path, des_filename):
    # 根据文件扩展名判断是处理 Excel 还是 CSV
    global latitude_col
    global longitude_col
    global results

    if file_path.endswith('.xlsx') or file_path.endswith('.xls'):
        df = pd.read_excel(file_path)
    elif file_path.endswith('.csv'):
        try:
            df = pd.read_csv(file_path, engine='python', encoding='utf-8')
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, engine='python', encoding='ANSI')

    # 自动识别经纬度列
    longitude_col, latitude_col = find_location_columns(df)

    # 初始化结果列表
    # 使用线程池处理每行数据
    with ThreadPoolExecutor(max_workers=5) as executor, open(des_filename, "a", newline="") as file:
        futures = []
        for index, row in df.iterrows():
            # 使用线程池提交任务
            futures.append(executor.submit(parse_single_location, row, index))

        # 等待所有线程完成
        for future in futures:
            result = future.result()
            if result is not None:
                results.append(result)

    
    # 将结果写入文件
    with open(des_filename, "a", newline="") as file:
        writer = csv.writer(file)
        for result in results:
            writer.writerow(result)


def parse_single_location(row, index):
    global api_key_now_idx
    global api_key
    global lock

    longitude = row[longitude_col]
    latitude = row[latitude_col]
    try:
        # 构建 API 请求
        url = f"https://restapi.amap.com/v3/geocode/regeo?output=xml&location={longitude},{latitude}&key={api_key}&radius=1000&extensions=all"
        response = requests.get(url)
        response_data_tree = html.fromstring(response.content)
        
        infocode = response_data_tree.xpath('//response/infocode')[0].text
        if infocode == "10003":
            with lock:
                logger.warning("访问超出上限，正在尝试换一个key")
                api_key_now_idx = api_key_now_idx + 1
                if api_key_now_idx < len(api_key_list):
                    api_key = api_key_list[api_key_now_idx]
                else:
                    logger.error(f"{index},{longitude},{latitude},访问超出上限")
                    return [index, longitude, latitude, "访问超出上限"]
        try:
            formatted_address = response_data_tree.xpath('//response/regeocode/formatted_address')[0].text
            logger.info(f"{index},{longitude},{latitude},{formatted_address}")
            return [index, longitude, latitude, formatted_address]
        except Exception as e:
            logger.error(f"{index},{longitude},{latitude},获取不到该地址")
            return [index, longitude, latitude, "获取不到该地址"]
    except Exception:
        logger.error(f"{index},{longitude},{latitude},获取不到该地址")
        return [index, longitude, latitude, "获取不到该地址,网络出错"]


if __name__ == "__main__":
    file_path = r"C:\Users\WalterJ726\Desktop\USGS_2008_2022_4.5.xlsx"  
    des_filename = "finalUSGS.csv"
    parseLocation(file_path, des_filename)