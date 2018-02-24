#!usr/bin/env python
#coding:utf-8

import requests, time, sys, os
from bs4 import BeautifulSoup
import pymongo
from collections import deque

# from get_proxies import get_arandom_ip
# 连接到MongoDB数据库
conn = pymongo.MongoClient('127.0.0.1', 27017)
bing_db = conn['second_web']
bing = bing_db['contents']

# 自动重试5次
def get_html(url, tries=5):
    # proxies = get_arandom_ip()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.108 Safari/537.36',
        'Accept-Language': 'en-US'
    }
    try:
        request = requests.get(url, headers=headers, timeout=60)
        # raise_for_status(), 如果不是 200 会抛出 HTTPError 错误
        request.raise_for_status()
        html = request.text
    except requests.HTTPError as e:
        html = None
        print(e)
        if tries > 0:
            # 如果不是 200 就重试，每次递减重试次数
            time.sleep(60)
            print('正在重试，还剩{}次'.format(tries))
            return get_html(url, tries - 1)
            # 如果 url 不存在会抛出 ConnectionError 错误，这个情况不做重试
    except requests.exceptions.ConnectionError as e:
        return
    return html

# 解析网页每条结果的标题与描述
def parse_html(html):
    base_url = 'http://www.bing.com{}'
    try:
        soup = BeautifulSoup(html, 'html.parser')
        titles = soup.select('.b_algo h2')
        descriptions = soup.select('.b_caption p')
        next_page = soup.select('li.b_pag a')[1:7] # 爬取7页 
        page_urls = list(map(lambda x: x.get('href'), next_page))
        page_urls = [base_url.format(page_url) for page_url in page_urls]
        titles = list(map(lambda x: x.get_text(), titles))
        descriptions = list(map(lambda x: x.get_text(), descriptions))
    except Exception as why:
        print(why)
        print('未匹配到')
        return
    return titles, descriptions, page_urls

# 写入数据库
def save_content(html):
    titles, descriptions, page_urls = parse_html(html)
    for t, d in zip(titles, descriptions):
        data = {
            'title': t,
            'description': d
        }
        bing.insert(data)
    for url in page_urls:
        html_content = get_html(url)
        titles, descriptions, page_urls = parse_html(html_content)
        for t, d in zip(titles, descriptions):
            data = {
                'title': t,
                'description': d
            }
            bing.insert(data)
    print('已写入数据库！')

# 通过关键词文件爬取
def parse_txt_to_url():
    keywords_deque = deque()
    with open('bing_keywords.txt', 'r') as f:
        content = f.read()
        if not content is None:
            for key in content.strip().split('\n'):
                if not '+' in key:
                    keyword = '+'.join(key.split())
                    keywords_deque.append(keyword)
                else:
                    keywords_deque.append(key)
        else:
            print('没有关键字了')
    return keywords_deque

# 通过从数据库中读取数据爬取
# def parse_db_to_url():
#     try:
#         sel_key = asphalt_bing_seen_keys.find().limit(-1).skip(0).next()  # 找到最上面的记录
#         asphalt_bing_seen_keys.delete_one(sel_key)  # 删掉查过的记录
#         keyword = sel_key['keyword']
#         with open('bing_seen_keywords.txt', 'a+') as f:
#             f.write(str(keyword) + '\n')  # 查过的关键字保存到txt里
#         q = '+'.join(keyword.split())
#     except Exception as why:
#         print(why)
#         return
#     return q

# 重启
def restart_program():
    python = sys.executable
    os.execl(python, python, *sys.argv)


if __name__ == '__main__':
    start = time.time()
    keywords = parse_txt_to_url()
    base_url = 'https://www.bing.com/search?q={}&ensearch=1'
    try:
        while keywords:
            q = keywords.popleft()
            if q is None:
                break
            url = base_url.format(q)
            html = get_html(url)
            print('正在获取链接{}的内容'.format(url))
            save_content(html)
    except Exception as why:
        print(why)
        with open('bing_keywords.txt','w') as f:
            while keywords:
                f.write(keywords.popleft()+'\n')
        print('出错了，未爬取的关键词保存了，正在等待2分钟后重启...')
        time.sleep(120)
        restart_program()
    else:
        print('爬取完毕！爬虫程序正常退出...')
    print('共耗时{}'.format(time.time() - start))
