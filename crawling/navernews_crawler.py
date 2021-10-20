import os
import sys
import requests
import re
from bs4 import BeautifulSoup as bs
import time
import random
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select

#headers = {
#    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36'}
#path = os.path.join(os.getcwd(), "chromedriver")


def crawling(chrome_driver_path: str):
    url = 'https://news.naver.com'
    url_add = url + '/main/ranking/offices.nhn'
    # res = requests.post(url_add, headers=headers)
    res = requests.post(url_add)
    press_link = []
    news_info = []
    soup = bs(res.text, 'html.parser')
    area = soup.find('ul', {'class': 'press_list'}).find_all('a', {'class': "nclicks('RBP.pname')"})
    data = []

    for part in area:
        press_link.append((part.text, part['href']))

    path = os.path.join(os.getcwd(), "chromedriver")
    options = webdriver.ChromeOptions()
    # options.add_argument('headless')
    # options.add_argument('user-agent=' + headers['User-Agent'])
    options.add_argument('headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('disable-gpu')
    options.add_argument('lang=ko_KR')
    driver = webdriver.Chrome(path, options=options)

    for elem in press_link:
        press_url = url + elem[1]
        press = elem[0]
        driver.get(press_url)
        driver.implicitly_wait(10)
        req = driver.page_source
        soup_tmp = bs(req, 'html.parser')
        #         res = requests.post(press_url, headers=headers)
        #         soup_tmp = bs(res.text,'html.parser')
        press_news = soup_tmp.find('ul', {'class': 'rankingnews_list type_detail'}).find_all('li')
        #         print(press_news)
        for news in press_news:
            try:
                news_url = news.find('a', {'class': "list_img nclicks('RBP.drnknws')"})['href']
                view_count = news.find('span', {'class': 'list_view'}).text
                view_count = int(view_count.replace(',', ''))
                rank = int(news.find('em', {'class': 'list_ranking_num'}).text)
                news_info.append((news_url, view_count, rank))
            except:
                pass
        time.sleep(random.uniform(1, 3))
    print('news link ready..')
    #     return news_info
    for news in news_info:
        news_url = url + news[0]
        driver.get(news_url)
        driver.implicitly_wait(10)
        req = driver.page_source
        soup = bs(req, 'html.parser')

        try:
            press = soup.find('div', {'class': 'press_logo'}).find('img')['title']
            title = soup.find('div', {'class': 'article_info'}).find('h3').text
            date = soup.find('div', {'class': 'sponsor'}).find('span', {'class': 't11'}).text
        except:
            press = None
            title = None
            date = None
        #         reactions = soup.find('ul',{'class':'u_likeit_layer _faceLayer'})

        driver.get(news_url)
        driver.implicitly_wait(10)
        time.sleep(5)
        rc = dict()
        for num in range(5):
            driver.implicitly_wait(10)
            elem = driver.find_element_by_xpath('//*[@id="spiLayer"]/div[1]/ul/li[' + str(num + 1) + ']/a/span[2]')
            if num == 0:
                rc['good'] = elem.text
            elif num == 1:
                rc['warm'] = elem.text
            elif num == 2:
                rc['sad'] = elem.text
            elif num == 3:
                rc['angry'] = elem.text
            else:
                rc['want'] = elem.text
        try:
            driver.implicitly_wait(10)
            n_comment = driver.find_element_by_xpath('//*[@id="cbox_module"]/div[2]/div[1]/a/span[1]')
            n_comment = int(n_comment.text.replace(',', ''))
        except:
            n_comment = -1
        data.append(
            {
                "rank": news[2],
                "date": date,
                "url": news_url,
                "title": title,
                "press": press,
                "n_reaction_good": rc['good'],
                "n_reaction_warm": rc['warm'],
                "n_reaction_sad": rc['sad'],
                "n_reaction_angry": rc['angry'],
                "n_reaction_want": rc['want'],
                "n_view": news[1],
                "n_comment": n_comment
            })
    return news_info, data

if __name__ == "__main__":
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36'
    }
    #chrome_driver_path = os.path.join(os.getcwd(), "chromedriver")
    chrome_driver_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "chromedriver")
    crawling(chrome_driver_path)
    # crawling(chrome_driver_path)
