# 다음 랭킹 뉴스 크롤러
# 많이 본 / 열독률 높은 / 댓글 많은 / 연령별 랭킹 뉴스

import re
import time

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from fake_useragent import UserAgent
import json
import os
import tqdm

# 많이 본 / 열독률 높은 / 댓글 많은 뉴스 순서
def crawl_content(soup, driver, category, ranking_box_class, num):
    ranking_box = soup.find_all(class_=ranking_box_class)
    contents_box = soup.find_all(class_="cont_thumb")
    lst = []

    # 기사 외부 정보 가져오기
    for num in range(num):
        d = dict()
        d['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))  # 크롤링한 날짜
        d['rank'] = int(ranking_box[num].find(class_="screen_out").get_text())  # 순위
        d['category'] = category

        news_info = contents_box[num].find(class_="tit_thumb")
        d['url'] = news_info.find('a')['href']  # url
        d['title'] = news_info.find('a').get_text()  # 제목
        d['press'] = news_info.find(class_='info_news').get_text()  # 언론사

        lst.append(d)

    # 기사 내부 정보 가져오기
    for link in lst:
        resp = requests.get(link['url'])
        soup = BeautifulSoup(resp.text, "lxml")
        info = soup.find(class_='info_view')
        link['date'] = info.find(class_='num_date').get_text()  # 입력날짜

        driver.get(link['url'])
        driver.implicitly_wait(3)
        print("내부")
        try:
            link['n_comment'] = int(driver.find_element_by_css_selector('#alex-header > em').text)  # 댓글수
        except:
            link['n_comment'] = -1

        foot = driver.find_element_by_css_selector(
            '#mArticle > div.foot_view > div.emotion_wrap > div.emotion_list > div > div > div')
        link['n_reaction_recommend'] = int(foot.find_element_by_css_selector(
            'div.selectionbox.type-RECOMMEND.unselected > span.count').text)  # 추천해요 수
        link['n_reaction_like'] = int(foot.find_element_by_css_selector(
            'div.selectionbox.type-LIKE.unselected > span.count').text)  # 좋아요 수
        link['n_reaction_impress'] = int(foot.find_element_by_css_selector(
            'div.selectionbox.type-IMPRESS.unselected > span.count').text)  # 감동이에요 수
        link['n_reaction_angry'] = int(foot.find_element_by_css_selector(
            'div.selectionbox.type-ANGRY.unselected > span.count').text)  # 화나요 수
        link['n_reaction_sad'] = int(foot.find_element_by_css_selector(
            'div.selectionbox.type-SAD.unselected > span.count').text)  # 슬퍼요 수
        # 전체 반응 수
        link['n_reactions'] = str(link['n_reaction_recommend'] + link['n_reaction_like'] \
                              + link['n_reaction_impress'] + link['n_reaction_angry'] \
                              + link['n_reaction_sad'])
        print(link)
    return lst

# 연령별 뉴스
def crawl_content_by_age(soup, driver, category):
    female = soup.find_all(class_='rank_female')
    male = soup.find_all(class_='rank_male')
    ranking_news = [female, male]
    lst = []

    for news in ranking_news:
        for i in range(4):  # {20대, 30대, 40대, 50대}
            l = []
            age = news[i].find(class_='txt_news').get_text()[:3]  # ex) "20대 여성"에서 앞에 두 글자
            sex = news[i].find(class_='txt_news').get_text()[4:]  # ex) "20대 여성"에서 마지막 두 글자
            news_list = news[i].find_all(class_='link_txt')
            press_list = news[i].find_all(class_='info_news')

            # 기사 외부 정보 가져오기
            # 한 섹션에 5위까지 나와있다.
            for num in range(5):
                d = dict()
                d['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))  # 크롤링한 날짜
                d['category'] = category  # 카테고리
                d['age'] = age  # 연령대
                d['sex'] = sex  # 성별
                d['rank'] = num + 1  # 순위
                d['url'] = news_list[num]['href']  # url
                d['title'] = news_list[num].get_text()  # 제목
                d['press'] = press_list[num].get_text()  # 언론사
                l.append(d)

            # 기사 내부 정보 가져오기
            for link in l:
                resp = requests.get(link['url'])
                time.sleep(1)
                soup = BeautifulSoup(resp.text, "lxml")
                info = soup.find(class_='info_view')
                link['date'] = info.find(class_='num_date').get_text()  # 입력날짜

                driver.get(link['url'])
                driver.implicitly_wait(3)
                print("내부")
                try:
                    link['n_comment'] = int(driver.find_element_by_css_selector('#alex-header > em').text)  # 댓글수
                except:
                    link['n_comment'] = -1

                foot = driver.find_element_by_css_selector(
                    '#mArticle > div.foot_view > div.emotion_wrap > div.emotion_list > div > div > div')
                link['n_reaction_recommend'] = int(foot.find_element_by_css_selector(
                    'div.selectionbox.type-RECOMMEND.unselected > span.count').text)  # 추천해요 수
                link['n_reaction_like'] = int(foot.find_element_by_css_selector(
                    'div.selectionbox.type-LIKE.unselected > span.count').text)  # 좋아요 수
                link['n_reaction_impress'] = int(foot.find_element_by_css_selector(
                    'div.selectionbox.type-IMPRESS.unselected > span.count').text)  # 감동이에요 수
                link['n_reaction_angry'] = int(foot.find_element_by_css_selector(
                    'div.selectionbox.type-ANGRY.unselected > span.count').text)  # 화나요 수
                link['n_reaction_sad'] = int(foot.find_element_by_css_selector(
                    'div.selectionbox.type-SAD.unselected > span.count').text)  # 슬퍼요 수
                # 전체 반응 수
                link['n_reactions'] = str(link['n_reaction_recommend'] + link['n_reaction_like'] \
                                      + link['n_reaction_impress'] + link['n_reaction_angry'] \
                                      + link['n_reaction_sad'])
        lst.append(l)
        print(l)
    return lst


def crawling(chrome_driver_path: str):
    # init chrome driver
    chrome_driver = chrome_driver_path
    chrome_options = webdriver.ChromeOptions()

    chrome_options.add_argument('headless')
    userAgent = UserAgent().random
    chrome_options.add_argument(f"user-agent={userAgent}")
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    
    prefs = {
        "profile.default_content_setting_values": {
            "cookies": 2,
            "images": 2,
            "plugins": 2,
            "popups": 2,
            "geolocation": 2,
            "notifications": 2,
            "auto_select_certificate": 2,
            "fullscreen": 2,
            "mouselock": 2,
            "mixed_script": 2,
            "media_stream": 2,
            "media_stream_mic": 2,
            "media_stream_camera": 2,
            "protocol_handlers": 2,
            "ppapi_broker": 2,
            "automatic_downloads": 2,
            "midi_sysex": 2,
            "push_messaging": 2,
            "ssl_cert_decisions": 2,
            "metro_switch_to_desktop": 2,
            "protected_media_identifier": 2,
            "app_banner": 2,
            "site_engagement": 2,
            "durable_storage": 2,
        }
    }
    chrome_options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(chrome_driver, options=chrome_options)

    date = time.strftime('%Y%m%d', time.localtime(time.time()))
    tags = ["popular/news", "popular/entertain", "popular/sports", "kkomkkom/news",
            "kkomkkom/entertain", "kkomkkom/sports", "bestreply/", "age/"]
    news_list = []
    print(date)
    # for each catogory
    for tag in tags:
        print(tag)
    # for i in tqdm(range(len(tags))):
       # tag = tags[i]
        url = "https://news.daum.net/ranking/" + tag + "?regDate=" + str(date)
        resp = requests.get(url)
        soup = BeautifulSoup(resp.text, "lxml")

        main_tag, category_tag = tag.split('/')
        if main_tag == "popular":
            news_list.extend(crawl_content(soup, driver, tag, 'rank_num rank_popular', 50))
        elif main_tag == "kkomkkom":
            news_list.extend(crawl_content(soup, driver, tag, 'rank_num rank_popular', 30))
        elif main_tag == "bestreply":
            news_list.extend(crawl_content(soup, driver, main_tag, 'rank_num', 50))
        elif main_tag == "age":
            news_list.extend(crawl_content_by_age(soup, driver, main_tag))
    return news_list


if __name__ == "__main__":
    chrome_driver_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "chromedriver")
    start = time.perf_counter()
    data = crawling(chrome_driver_path)
    end = time.perf_counter()
    print('time elapsed: ', end-start)
    print('data size: ', len(data))
    title = "Daum_ranking_news_" + str(time.strftime('%Y%m%d_%H%M%S', time.localtime(time.time()))) + ".json"
    with open(title, 'w', encoding='utf-8') as make_file:
        json.dump(data, make_file, indent="\t", ensure_ascii=False)

