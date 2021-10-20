import os
import sys
sys.path.append("/home/capje/kafka_tool/")
sys.path.append("/home/capje/neo4j_query/")

from test_ver_3 import naver_neo4j, daum_neo4j, youtube_neo4j, add_attribute, feature_importance, keyword_info
from kafka_module import Consumer
import datetime
from tqdm import tqdm

from config import CONFIG
from config import TEST_CONFIG

from itertools import combinations as comb

if __name__ == "__main__":
    
    contents_type = sys.argv[1]

    consumer = Consumer("/home/capje/kafka_tool/config.yaml", "neo4j_insert1", value_type="json")
    time = "T" + datetime.datetime.now().strftime("%Y_%m_%d_%H")

    if contents_type == "naver_news":

        app = naver_neo4j.App(CONFIG["bolt_url"], CONFIG["user"], CONFIG["password"])
        platform = 'Naver'

        data = consumer.get_data(topic='naver_news_2')

        json_data = [d.value for d in data]
        
        for value in tqdm(json_data):
            app.create_creator_and_match_platform(value['press'], platform)
            crawl_time = value['crawl_time'].replace('-', '_')
            crawl_time = 'T'+ crawl_time.replace('/', '_')
            
            app.create_content_and_match_with_info(value['url'], value['title'], value['date'], value['n_view'], value['n_comment'], \
                value['n_reactions'], value['n_reaction_good'], value['n_reaction_warm'], value['n_reaction_sad'], \
                value['n_reaction_angry'], value['n_reaction_want'], \
                crawl_time, 'bestview', value['rank'], value['press'], platform)

            for elem in value['keyword']:
                app.create_keyword(elem, crawl_time)
                app.match_content_and_keyword(elem, value['url'], crawl_time)
            
            # keyword 양방향 연결
            keyL = list(comb(value['keyword'],2))
            for ll in keyL:
                app.match_keywords(ll[0],ll[1], crawl_time)
                
        app.close()



    elif contents_type == "daum_news":

        app = daum_neo4j.App(CONFIG["bolt_url"], CONFIG["user"], CONFIG["password"])
        platform = 'Daum'

        data = consumer.get_data(topic='daum_news_2')

        json_data = [d.value for d in data]
        
        for value in tqdm(json_data):
            app.create_creator_and_match_platform(value['press'], platform)
            crawl_time = value['crawl_time'].replace('-', '_')
            crawl_time = 'T'+ crawl_time.replace('/', '_')

            if value['category'] == 'age':
                app.create_content_and_match_with_info_classified_by_age(value['url'], value['title'], value['date'], value['n_comment'], \
                    value['n_reactions'], value['n_reaction_recommend'], value['n_reaction_like'], value['n_reaction_impress'], \
                    value['n_reaction_angry'], value['n_reaction_sad'], crawl_time, \
                    value['category'], value['age'], value['sex'], value['rank'], value['press'], platform)

            else:
                app.create_content_and_match_with_info(value['url'], value['title'], value['date'], value['n_comment'], \
                    value['n_reactions'], value['n_reaction_recommend'], value['n_reaction_like'], value['n_reaction_impress'], \
                    value['n_reaction_angry'], value['n_reaction_sad'], \
                    crawl_time, value['category'], value['rank'], value['press'], platform)

            for elem in value['keyword']:
                app.create_keyword(elem, crawl_time)
                app.match_content_and_keyword(elem, value['url'], crawl_time)

            # keyword 양방향 연결
            keyL = list(comb(value['keyword'],2))
            for ll in keyL:
                app.match_keywords(ll[0],ll[1], crawl_time)

        app.close()



    elif contents_type == "youtube_contents":

        app = youtube_neo4j.App(CONFIG["bolt_url"], CONFIG["user"], CONFIG["password"])
        platform = 'Youtube'

        data = consumer.get_data(topic='youtube_contents_2')
        
        json_data = [d.value for d in data]
        
        for value in tqdm(json_data):
            app.create_creator_and_match_platform(value['creator'], value['creator_url'], platform)
            crawl_time = value['crawl_time'].replace('-', '_')
            crawl_time = 'T'+ crawl_time.replace('/', '_')

            app.create_content_and_match_with_info(value['url'], value['title'], value['date'], value['n_views'], value['n_comment'], \
                value['n_reactions'], value['n_reaction_good'], value['n_reaction_bad'], value['hashtags'], value['description'], \
                crawl_time, 'korea_popular', value['rank'], value['creator'], value['creator_url'], platform)

            for elem in value['keyword']:
                app.create_keyword(elem, crawl_time)
                app.match_content_and_keyword(elem, value['url'], crawl_time)
            
            # keyword 양방향 연결
            keyL = list(comb(value['keyword'], 2))
            for ll in keyL:
                app.match_keywords(ll[0],ll[1], crawl_time)

        app.close()
    

    elif contents_type == "centrality":

        app = add_attribute.App(CONFIG["bolt_url"], CONFIG["user"], CONFIG["password"])
        name = "insert_graph"
        app.make_gds_graph(name,time)
        app.insert_pagerank(name)
        app.insert_betweeness(name)
        app.insert_closeness(name)
        app.insert_degree(time)
        app.insert_clustering(name)
        app.drop_gds_graph(name)
        app.close()  

    elif contents_type == "feature_importance":

        app = feature_importance.App(CONFIG["bolt_url"], CONFIG["user"], CONFIG["password"])
        output = dict()
        platforms = ['Naver', 'Daum', 'Youtube']

        for platform in platforms:
            df, feature = app.load_data(platform)
            rf = app.randomforest(df, feature).to_dict()
            output[platform] = rf
            rf = {platform : rf}
            app.insert_feature_importance(rf, time)    
    
        app.close()

    elif contents_type == "keyword_info":

        app = keyword_info.App(CONFIG["bolt_url"], CONFIG["user"], CONFIG["password"])
        app.keyword_info_fi(time)
        app.close()

    elif contents_type == "naver_news_test":

        app = naver_neo4j.App(TEST_CONFIG["bolt_url"], TEST_CONFIG["user"], TEST_CONFIG["password"])
        
        platform = 'Naver'

        data = consumer.get_data(topic='naver_news_2')
        json_data = [d.value for d in data]

        for value in tqdm(json_data):
            app.create_content_test(value['url'], value['title'], value['date'], value['n_view'], value['n_comment'], \
                value['n_reactions'], value['n_reaction_good'], value['n_reaction_warm'], value['n_reaction_sad'], \
                value['n_reaction_angry'], value['n_reaction_want'], \
                value['crawl_time'], 'bestview', value['rank'], value['press'], platform)

            for elem in value['keyword']:
                app.create_keyword(elem)
                app.match_content_and_keyword_test(elem, value['url'], value['crawl_time'])
            
            # keyword 양방향 연결
            from itertools import combinations as comb
            keyL = list(comb(value['keyword'],2))
            for ll in keyL:
                app.match_keywords(ll[0],ll[1])
            
                
                

