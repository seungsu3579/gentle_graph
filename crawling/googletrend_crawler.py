import re
import datetime
from selenium import webdriver
from fake_useragent import UserAgent
import os

def crawling(chrome_driver_path: str):
    # init chrome driver
    chrome_driver = chrome_driver_path

    chrome_options = webdriver.ChromeOptions()

    chrome_options.add_argument("headless")
    chrome_options.add_argument("--window-size=1100,2000")
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

    # go to google trends
    driver.get("https://trends.google.co.kr/trends/trendingsearches/daily?geo=KR")
    trends = driver.find_elements_by_css_selector("div.feed-list-wrapper")
    today_trends = trends[0].find_elements_by_css_selector("feed-item")

    trends_list = []
    for trend in today_trends:
        header = trend.find_element_by_css_selector("div.feed-item-header")

        rank = header.find_element_by_css_selector("div.index").text

        keyword = (
            header.find_element_by_css_selector("div.title")
            .find_element_by_css_selector("a")
            .text
        )

        news_url = header.find_element_by_css_selector(
            "div.summary-text > a"
        ).get_attribute("href")

        now = datetime.datetime.now()
        created_time = (
            header.find_element_by_css_selector("div.source-and-time")
            .find_elements_by_css_selector("span")[1]
            .text
        )
        if "시간" in created_time:
            created_time = int(re.findall("\d+", created_time)[0])
            created_time = now - datetime.timedelta(hours=created_time)
        elif "일" in created_time:
            created_time = int(re.findall("\d+", created_time)[0])
            created_time = now - datetime.timedelta(days=created_time)
        else:
            created_time = now

        search_count = header.find_element_by_css_selector(
            "div.search-count-title"
        ).text

        data = {
            "rank": rank,
            "keyword": keyword,
            "news_url": news_url,
            "created_time": created_time.strftime("%Y-%m-%d"),
            "search_count": search_count,
        }

        print(data)
        trends_list.append(data)

    return trends_list


if __name__ == "__main__":
    chrome_driver_path = os.path.join(os.getcwd(), "chromedriver")
    result = crawling(chrome_driver_path)
    # print(trends_list)
