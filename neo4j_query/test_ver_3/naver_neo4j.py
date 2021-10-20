from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable
from tqdm import tqdm
from itertools import combinations as comb

class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    def create_creator_and_match_platform(self, creator, platform):
        '''create creator and match with platform'''
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_creator, creator)
            if len(result) == 0:
                session.write_transaction(self._create_creator, creator)
            result = session.read_transaction(self._find_edge_with_creator_and_platform, creator, platform)
            if len(result) == 0:
                session.write_transaction(self._match_creator_and_platform, creator, platform)

    def create_content_and_match_with_info(self, url, title, date, n_view, n_comment, n_reactions, good, warm, sad, angry, want, crawl_time, category, rank, creator, platform):
        '''create content and match with creator, type, rank'''
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_content, url, crawl_time)
            if len(result) == 0:
                result = session.write_transaction(
                    self._create_content_and_match_with_info, url, title, date, n_view, n_comment, n_reactions, good, warm, sad, angry, want, crawl_time, category, rank, creator, platform
                )
            else:
                result = session.write_transaction(
                    self._match_content_with_info, url, crawl_time, category, rank, platform
                )

    def create_keyword(self, keyword, crawl_time):
        '''create keyword'''
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_keyword, keyword, crawl_time)
            if len(result) == 0:
                result = session.write_transaction(self._create_keyword, keyword, crawl_time)

    def match_content_and_keyword(self, keyword, url, crawl_time ):
        '''match content and content, match content and keyword'''
        with self.driver.session() as session:
            result = session.read_transaction(
                self._find_content_and_keyword, keyword, url, crawl_time
            )
            # content와 keyword간의 연결이 없을 경우
            if len(result) == 0:
                # 해당 keyword에 연결된 나를 제외한 모든 content 찾기
                contents = session.read_transaction(self._find_content_extracts_keyword, keyword, crawl_time)
                for content in contents:
                    urlC, [contentC, crawl_timeC] = content
                    link = session.read_transaction(self._find_content_and_content,urlC, contentC, crawl_timeC, url, crawl_time)
                    # 나와 공통된 keyword를 가진 다른 content와의 연결이 없을 경우, 연결 생성
                    if len(link) == 0:
                        session.write_transaction(self._match_content_and_content,urlC, contentC, crawl_timeC, url, crawl_time)
                    # 나와 공통된 keyword를 가진 다른 content와의 연결이 있을 경우, weight 업데이트
                    else :
                        keywords_other = session.read_transaction(self._find_keyword_extracted_content, urlC, contentC, crawl_timeC)
                        keywords_my = session.read_transaction(self._find_keyword_extracted_content, url, 'Content', crawl_time)
                        count = 1
                        for k in keywords_my:
                            if k in keywords_other:
                               count = count + 1
                        session.write_transaction(self._update_content_and_content,urlC, contentC, crawl_timeC, url, crawl_time, count)
                # content와 keyword 간의 연결 생성
                session.write_transaction(
                    self._match_content_and_keyword, keyword, url, crawl_time
                )   

    # keyword connection
    def match_keywords(self, keyword1, keyword2, crawl_time):
        '''match or update keyword and keyword'''
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_keyword_connection, keyword1, keyword2, crawl_time)
            if len(result) == 0:
                session.write_transaction(self._match_keywords, keyword1, keyword2, crawl_time)
            else:
                session.write_transaction(self._update_keywords, keyword1, keyword2, crawl_time)

    @staticmethod
    def _find_and_return_creator(tx, creator):
        '''create_creator_and_match_platform method'''
        query = (
            "MATCH (c:Creator { name: $creator, type: 'press' }) "
            "RETURN c.name AS name "
        )
        result = tx.run(query, creator=creator)
        res = [row["name"] for row in result]
        if res == None:
            return None
        else:
            return res

    @staticmethod
    def _create_creator(tx, creator):
        '''create_creator_and_match_platform method'''
        query = (
            "CREATE (c: Creator { name: $creator, type: 'press' }) "
            "return c"
        )
        tx.run(query, creator=creator)

    @staticmethod
    def _find_edge_with_creator_and_platform(tx, creator, platform):
        '''create_creator_and_match_platform method'''
        query = (
            "MATCH (c:Creator)-[:USES]->(p:Platform) "
            "WHERE c.name = $creator AND c.type = 'press' AND p.name = $platform "
            "RETURN c.name AS name "
        )
        result = tx.run(query, creator=creator, platform=platform)
        res = [row["name"] for row in result]
        if res == None:
            return None
        else:
            return res

    @staticmethod
    def _match_creator_and_platform(tx, creator, platform):
        '''create_creator_and_match_platform method'''
        query = (
            "MATCH (c: Creator { name: $creator, type: 'press' }) "
            "MATCH (p: Platform { name: $platform }) "
            "CREATE (c)-[:USES]->(p)"
        )
        tx.run(query, creator=creator, platform=platform)
    
    @staticmethod
    def _find_and_return_content(tx, url, crawl_time):
        '''create_content_and_match_with_info method'''
        query = (
            "MATCH (c:Content:" + crawl_time + " { url: $url }) "
            "RETURN c.title AS title"
        )
        result = tx.run(query, url=url)
        res = [row["title"] for row in result]
        if res == None:
            return None
        else:
            return res

    @staticmethod
    def _create_content_and_match_with_info(tx, url, title, date, n_view, n_comment, n_reactions, good, warm, sad, angry, want, crawl_time, category, rank, creator, platform):
        '''create_content_and_match_with_info method'''
        query = (
            "CREATE (c:Content:" + crawl_time + " { url: $url, title: $title, date: $date, n_view: $n_view, n_comment: $n_comment, n_reactions: $n_reactions, good: $good, warm: $warm, sad: $sad, angry: $angry, want: $want }) "
            "WITH c "
            "MATCH (t:Type { name: $category  }) "
            "MATCH (r:Rank { n: $rank, platform: $platform }) "
            "MATCH (cr:Creator) WHERE cr.name = $creator AND cr.type = 'press' "
            "MERGE (c)<-[:INCLUDES]-(t) "
            "MERGE (c)<-[:RANKS]-(r) "
            "MERGE (c)<-[:MAKES]-(cr) "
        )
        result = tx.run(query, url=url, title=title, date=date, n_view=n_view, n_comment=n_comment,\
            n_reactions=n_reactions, good=good, warm=warm, sad=sad, angry=angry, want=want, \
                category=category, rank=rank, creator=creator, platform=platform)

    @staticmethod
    def _match_content_with_info(tx, url, crawl_time, category, rank, platform):
        '''create_content_and_match_with_info method'''
        query = (
            "MATCH (c: Content:" + crawl_time + " { url: $url }) "
            "MATCH (t:Type { name: $category }) "
            "MATCH (r:Rank { n: $rank, platform: $platform }) "
            "MERGE (c)<-[:INCLUDES]-(t) "
            "MERGE (c)<-[:RANKS]-(r) "
        )
        result = tx.run(query, url=url, category=category, rank=rank, platform=platform)

    @staticmethod
    def _find_and_return_keyword(tx, keyword, crawl_time):
        '''create_keyword method'''
        query = (
            "MATCH (k:Keyword:" + crawl_time + ") "
            "WHERE k.name = $keyword "
            "RETURN k.name AS name "
        )
        result = tx.run(query, keyword=keyword)
        res = [row["name"] for row in result]
        if res == None:
            return None
        else:
            return res

    @staticmethod
    def _create_keyword(tx, keyword, crawl_time):
        '''create_keyword method'''
        query = (
            "CREATE (k:Keyword:" + crawl_time + " { name: $keyword }) "
            "RETURN k "
            )
        result = tx.run(query, keyword=keyword)

    @staticmethod
    def _find_content_and_keyword(tx, keyword, url, crawl_time ):
        '''match_content_and_keyword method'''
        query = (
            "MATCH (c: Content:" + crawl_time + " { url: $url }) "
            "WITH c "
            "MATCH (c)-[:EXTRACTS]->(k: Keyword:" + crawl_time + " { name: $keyword }) "
            "RETURN k.name AS name"
            )
        result = tx.run(query, keyword=keyword, url=url)
        res = [row["name"] for row in result]
        if res == None:
            return None
        else:
            return res

    @staticmethod
    def _find_content_extracts_keyword(tx, keyword, crawl_time):
        '''match_content_and_keyword method'''
        query = (
            "MATCH (k:Keyword:" + crawl_time + " { name: $keyword })<-[:EXTRACTS]-(c:Content:" + crawl_time + ") "
            "RETURN labels(c) as labels, c.url as url "
        )
        result = tx.run(query, keyword=keyword)
        res = [[row["url"],row['labels']] for row in result]
        if res == None:
            return None
        else:
            return res

    @staticmethod
    def _find_content_and_content(tx, urlC, contentC, crawl_timeC, url, crawl_time):
        '''match_content_and_keyword method'''
        query = (
            "MATCH (c1:Content:"+crawl_time+")-[:LINKS]->(c2:"+contentC+":"+crawl_timeC+" { url: $urlC }) "
            "WHERE c1.url = $url "
            "RETURN c2.url as url"
        )
        result = tx.run(query, urlC=urlC, url=url)
        res = [row["url"] for row in result]
        if res == None:
            return None
        else:
            return res
            
    @staticmethod
    def _match_content_and_content(tx, urlC, contentC, crawl_timeC, url, crawl_time):
        '''match_content_and_keyword method'''
        query = (
            "MATCH (c1:"+contentC+":"+crawl_time+") "
            "WHERE c1.url = $urlC "
            "MATCH (c2:Content:"+crawl_time+") "
            "WHERE c2.url = $url "
            "CREATE (c1)-[:LINKS { weight: 1 }]->(c2) "
            "CREATE (c2)-[:LINKS { weight: 1 }]->(c1) "
        )
        tx.run(query, urlC=urlC, url=url) 

    @staticmethod
    def _find_keyword_extracted_content(tx, url, content, crawl_time):
        '''match_content_and_keyword method'''
        query = (
            "MATCH (c:"+content+":"+crawl_time+" { url: $url })-[:EXTRACTS]->(k: Keyword:"+crawl_time+") "
            "RETURN k.name as name"
        )
        result = tx.run(query, url=url)
        res = [row['name'] for row in result]
        if res == None:
            return None
        else:
            return res

    @staticmethod
    def _update_content_and_content(tx, urlC, contentC, crawl_timeC, url, crawl_time, count):
        '''match_content_and_keyword method'''
        query = (
            "MATCH (c1:"+contentC+":"+crawl_timeC+" { url: $urlC }) "
            "MATCH (c2:Content"+crawl_time+" { url: $url }) "
            "MATCH (c1)-[l1:LINKS]->(c2) "
            "MATCH (c2)-[l2:LINKS]->(c1) "
            "SET l1.weight = $count, l2.weight = $count "
            "RETURN l1, l2"
        )
        tx.run(query, urlC=urlC, url=url, count=count) 

    @staticmethod
    def _match_content_and_keyword(tx, keyword, url, crawl_time):
        '''match_content_and_keyword method'''
        query = (
            "MATCH (c: Content:"+crawl_time+" { url: $url }) "
            "WITH c "
            "MATCH (k: Keyword:"+crawl_time+" { name: $keyword}) "
            "CREATE (c)-[:EXTRACTS]->(k)"
            )
        result = tx.run(query, keyword=keyword, url=url)

    @staticmethod
    def _find_and_return_keyword_connection(tx, keyword1, keyword2, crawl_time):
        '''match_keywords method'''
        query = (
            "MATCH (k1:Keyword:"+crawl_time+" { name: $keyword1 }) "
            "MATCH (k2:Keyword:"+crawl_time+" { name: $keyword2 }) "
            "MATCH (k1)-[c1:CONNECTS]->(k2) "
            "RETURN k1.name AS name "
        )
        result = tx.run(query, keyword1=keyword1, keyword2=keyword2)
        res = [row["name"] for row in result]
        if res == None:
            return None
        else:
            return res

    @staticmethod
    def _match_keywords(tx, keyword1, keyword2, crawl_time):
        '''match_keywords method'''
        query = (
            "MATCH (k1:Keyword:"+crawl_time+" { name: $keyword1 }) "
            "MATCH (k2:Keyword:"+crawl_time+" { name: $keyword2 }) "
            "CREATE (k1)-[:CONNECTS { weight: 1 }]->(k2)"
            "CREATE (k2)-[:CONNECTS { weight: 1 }]->(k1)"
        )
        tx.run(query, keyword1=keyword1, keyword2=keyword2)

    @staticmethod
    def _update_keywords(tx, keyword1, keyword2, crawl_time):
        '''match_keywords method'''
        query = (
            "MATCH (k1:Keyword:"+crawl_time+" { name: $keyword1 }) "
            "MATCH (k2:Keyword:"+crawl_time+" { name: $keyword2 }) "
            "MATCH (k1)-[c1:CONNECTS]->(k2) "
            "MATCH (k2)-[c2:CONNECTS]->(k1) "
            "SET c1.weight = c1.weight + 1, c2.weight = c2.weight + 1 "
            "RETURN c1, c2"
        )
        tx.run(query, keyword1=keyword1, keyword2=keyword2)


if __name__ == "__main__":
    # Aura queries use an encrypted connection using the "neo4j+s" URI scheme
    bolt_url = "bolt://localhost:7687"
    user = "neo4j"
    password = "neo4j123"
    app = App(bolt_url, user, password)
    #data part
    platform = 'Naver'
    import os
    print(os.getcwd())
    import json
    
    # data 파일 여러개 
    path = '/home/capje/crawling/pre_data/'
    file_list = os.listdir(path)
    file_list_py = [file for file in file_list if file.startswith('pre_naver_data')]
    for i in file_list_py:
        print(path+i)
        with open((path+i), 'r') as f:
            json_data = json.load(f)
            print(len(json_data))
        
    # with open("/home/capje/tmp_data/naver_data_C_pre.json",'r') as f:
    #     json_data = json.load(f)
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
