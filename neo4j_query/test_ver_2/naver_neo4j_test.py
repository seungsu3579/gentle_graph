from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable
from tqdm import tqdm

class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    def create_crawltime(self, crawl_time):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_crawltime, crawl_time)
            if len(result) == 0:
                session.write_transaction(self._create_crawltime, crawl_time)

    def create_creator_and_match_platform(self, creator, platform):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_creator, creator)
            if len(result) == 0:
                session.write_transaction(self._create_creator, creator)
            result = session.read_transaction(self._find_edge_with_creator_and_platform, creator, platform)
            if len(result) == 0:
                session.write_transaction(self._match_creator_and_platform, creator, platform)

    def create_content_and_match_with_info(self, url, title, date, n_view, n_comment, good, warm, sad, angry, want, crawl_time, category, rank, creator, platform):
        with self.driver.session() as session:
            # 해당 crawl_time에 content가 존재하는지 확인
            result = session.read_transaction(self._find_and_return_content, url, crawl_time)
            if len(result) == 0:
                result = session.write_transaction(
                    self._create_content_and_match_with_info, url, title, date, n_view, n_comment, good, warm, sad, angry, want, crawl_time, category, rank, creator, platform
                )
            else:
                result = session.write_transaction(
                    self._match_content_with_info, url, crawl_time, category, rank, platform
                )

    def create_keyword(self, keyword):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_keyword, keyword)
            if len(result) == 0:
                result = session.write_transaction(self._create_keyword, keyword)

    def match_content_and_keyword(self, keyword, url, crawl_time):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_content_and_keyword, keyword, url, crawl_time)
            if len(result) == 0:
                result = session.write_transaction(
                    self._match_content_and_keyword, keyword, url, crawl_time
                )
                
    def match_content_and_keyword_test(self, keyword, url, crawl_time ):
        with self.driver.session() as session:
            result = session.read_transaction(
                self._find_content_and_keyword_test, keyword, url, crawl_time
            )
            if len(result) == 0:
                contents = session.read_transaction(self._find_content_extracts_keyword,keyword)
                for content in contents:
                    urlC, crawl_timeC = content
                    # if 연결고리가 없는 경우
                    link = session.read_transaction(self._find_content_and_content,urlC, crawl_timeC, url, crawl_time)
                    if len(link) == 0:
                        session.write_transaction(self._match_content_and_content,urlC, crawl_timeC, url, crawl_time)
                    else :
                        keywords_other = session.read_transaction(self._find_keyword_extracted_content, urlC, crawl_timeC)
                        keywords_my = session.read_transaction(self._find_keyword_extracted_content, url, crawl_time)
                        
                        count = 1
                        for k in keywords_my:
                            if k in keywords_other:
                               count = count + 1

                        session.write_transaction(self._update_content_and_content,urlC, crawl_timeC, url, crawl_time, count)

            session.write_transaction(
                self._match_content_and_keyword_test, keyword, url, crawl_time
            )   

    # 모든 항목 content의 property로 넣기
    def create_content_test(self, url, title, date, n_view, n_comment, good, warm, sad, angry, want, crawl_time, category, rank, creator, platform):
        with self.driver.session() as session:
            session.write_transaction(
                self._create_content_test, url, title, date, n_view, n_comment, good, warm, sad, angry, want, crawl_time, category, rank, creator, platform
            )

    # keyword connection
    def match_keywords(self, keyword1, keyword2):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_keyword_connection, keyword1, keyword2)
            if len(result) == 0:
                session.write_transaction(
                    self._match_keywords, keyword1, keyword2
                )
            else:
                session.write_transaction(
                    self._update_keywords, keyword1, keyword2
                )

    @staticmethod
    def _find_content_and_content(tx, urlC, crawl_timeC, url, crawl_time):
        query = (
            "MATCH (c1:Content)-[:LINKS]->(c2:Content { url: $urlC, crawl_time: $crawl_timeC }) "
            "WHERE c1.url = $url AND c1.crawl_time = $crawl_time "
            "RETURN c2.url as url"
        )
        result = tx.run(query, urlC=urlC, crawl_timeC=crawl_timeC, url=url, crawl_time=crawl_time)
        res = [row["url"] for row in result]
        if res == None:
            return None
        else:
            return res
            
    @staticmethod
    def _match_content_and_content(tx, urlC, crawl_timeC, url, crawl_time):
        query = (
            "MATCH (c1:Content) "
            "WHERE c1.url = $urlC AND c1.crawl_time = $crawl_timeC "
            "MATCH (c2:Content) "
            "WHERE c2.url = $url AND c2.crawl_time = $crawl_time "
            "CREATE (c1)-[:LINKS { weight: 1 }]->(c2)"
            "CREATE (c2)-[:LINKS { weight: 1 }]->(c1)"
        )
        tx.run(query, urlC=urlC, crawl_timeC=crawl_timeC, url=url, crawl_time=crawl_time) 

    @staticmethod
    def _update_content_and_content(tx, urlC, crawl_timeC, url, crawl_time, count):
        query = (
            "MATCH (c1:Content { url: $urlC, crawl_time: $crawl_timeC}) "
            "MATCH (c2:Content { url: $url, crawl_time: $crawl_time }) "
            "MATCH (c1)-[l1:LINKS]->(c2) "
            "MATCH (c2)-[l2:LINKS]->(c1) "
            "SET l1.weight = $count, l2.weight = $count "
            "RETURN l1, l2"
        )
        tx.run(query, urlC=urlC, crawl_timeC=crawl_timeC, url=url, crawl_time=crawl_time, count=count) 

    @staticmethod
    def _find_content_extracts_keyword(tx, keyword):
        query = (
            "MATCH (k:Keyword { name: $keyword })<-[:EXTRACTS]-(c:Content) "
            "RETURN c.url as url, c.crawl_time as crawl_time "
        )
        result = tx.run(query, keyword=keyword)
        res = [[row["url"],row['crawl_time']] for row in result]
        if res == None:
            return None
        else:
            return res

    @staticmethod
    def _find_keyword_extracted_content(tx, urlC, crawl_timeC):
        query = (
            "MATCH (c: Content { url: $urlC, crawl_time: $crawl_timeC })-[:EXTRACTS]->(k: Keyword) "
            "RETURN k.name as name"
        )
        result = tx.run(query, urlC=urlC, crawl_timeC=crawl_timeC)
        res = [row['name'] for row in result]
        if res == None:
            return None
        else:
            return res

    @staticmethod
    def _match_keywords(tx, keyword1, keyword2):
        query = (
            "MATCH (k1:Keyword { name: $keyword1 }) "
            "MATCH (k2:Keyword { name: $keyword2 }) "
            "CREATE (k1)-[:CONNECTS { weight: 1 }]->(k2)"
            "CREATE (k2)-[:CONNECTS { weight: 1 }]->(k1)"
        )
        tx.run(query, keyword1=keyword1, keyword2=keyword2)

    @staticmethod
    def _update_keywords(tx, keyword1, keyword2):
        query = (
            "MATCH (k1:Keyword { name: $keyword1 }) "
            "MATCH (k2:Keyword { name: $keyword2 }) "
            "MATCH (k1)-[c1:CONNECTS]->(k2) "
            "MATCH (k2)-[c2:CONNECTS]->(k1) "
            "SET c1.weight = c1.weight + 1, c2.weight = c2.weight + 1 "
            "RETURN c1, c2"
        )
        tx.run(query, keyword1=keyword1, keyword2=keyword2)

    @staticmethod
    def _find_and_return_keyword_connection(tx, keyword1, keyword2):
        query = (
            "MATCH (k1: Keyword { name: $keyword1 })-[:CONNECTS]->(k2: Keyword { name: $keyword2 }) "
            "RETURN k1.name AS name "
        )
        result = tx.run(query, keyword1=keyword1, keyword2=keyword2)
        res = [row["name"] for row in result]
        if res == None:
            return None
        else:
            return res

    @staticmethod
    def _find_and_return_crawltime(tx, crawl_time):
        query = (
            "MATCH (c:CrawlTime) "
            "WHERE c.time = $crawl_time "
            "RETURN c.time AS time "
        )
        result = tx.run(query, crawl_time=crawl_time)
        res = [row["time"] for row in result]
        if res == None:
            return None
        else:
            return res

    @staticmethod
    def _create_crawltime(tx, crawl_time):
        query = (
            "CREATE (c: CrawlTime { time: $crawl_time }) "
        )
        result = tx.run(query, crawl_time=crawl_time)

    @staticmethod
    def _find_and_return_creator(tx, creator):
        query = (
            "MATCH (c:Creator) "
            "WHERE c.name = $creator "
            "RETURN c.name AS name "
        )
        result = tx.run(query, creator=creator)
        res = [row["name"] for row in result]
        if res == None:
            return None
        else:
            return res

    @staticmethod
    def _find_edge_with_creator_and_platform(tx, creator, platform):
        query = (
            "MATCH (c:Creator)-[:USES]->(p:Platform) "
            "WHERE c.name = $creator AND p.name = $platform "
            "RETURN c.name AS name "
        )
        result = tx.run(query, creator=creator, platform=platform)
        res = [row["name"] for row in result]
        if res == None:
            return None
        else:
            return res

    @staticmethod
    def _create_creator(tx, creator):
        query = (
            "CREATE (c: Creator { name: $creator, type: 'press' }) "
            "return c"
        )
        result = tx.run(query, creator=creator)

    @staticmethod
    def _match_creator_and_platform(tx, creator, platform):
        query = (
            "MATCH (c: Creator { name: $creator, type: 'press' }) "
            "MATCH (p: Platform { name: $platform }) "
            "CREATE (c)-[:USES]->(p)"
        )
        result = tx.run(query, creator=creator, platform=platform)
    
    @staticmethod
    def _find_and_return_keyword(tx, keyword):
        query = (
            "MATCH (k:Keyword) "
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
    def _create_keyword(tx, keyword):
        query = (
            "CREATE (k: Keyword { name: $keyword }) "
            "RETURN k "
            )
        result = tx.run(query, keyword=keyword)

    @staticmethod
    def _find_content_and_keyword(tx, keyword, url, crawl_time ):
        query = (
            "MATCH (c: Content { url: $url })-[:CRAWLED_AT]->(ct: CrawlTime { time: $crawl_time}) "
            "WITH c "
            "MATCH (c)-[:EXTRACTS]->(k: Keyword { name: $keyword }) "
            "RETURN k.name AS name"
            )
        result = tx.run(query, keyword=keyword, url=url, crawl_time=crawl_time)
        res = [row["name"] for row in result]
        if res == None:
            return None
        else:
            return res

    @staticmethod
    def _find_content_and_keyword_test(tx, keyword, url, crawl_time ):
        query = (
            "MATCH (c: Content { url: $url, crawl_time: $crawl_time })-[:EXTRACTS]->(k: Keyword { name: $keyword }) "
            "RETURN c.url AS url"
            )
        result = tx.run(query, keyword=keyword, url=url, crawl_time=crawl_time)
        res = [row["url"] for row in result]
        if res == None:
            return None
        else:
            return res

    @staticmethod
    def _match_content_and_keyword(tx, keyword, url, crawl_time):
        query = (
            "MATCH (c: Content { url: $url })-[:CRAWLED_AT]->(ct: CrawlTime { time: $crawl_time}) "
            "WITH c "
            "MATCH (k: Keyword { name: $keyword}) "
            "CREATE (c)-[:EXTRACTS]->(k)"
            )
        result = tx.run(query, keyword=keyword, url=url, crawl_time=crawl_time)

    @staticmethod
    def _match_content_and_keyword_test(tx, keyword, url, crawl_time):
        query = (
            "MATCH (c: Content { url: $url, crawl_time : $crawl_time }) "
            "MATCH (k: Keyword { name: $keyword }) "
            "CREATE (c)-[:EXTRACTS]->(k) "
            )
        result = tx.run(query, keyword=keyword, url=url, crawl_time=crawl_time)

    @staticmethod
    def _find_and_return_content(tx, url, crawl_time):
        query = (
            "MATCH (c: Content { url: $url })-[:CRAWLED_AT]->(cr: CrawlTime { time: $crawl_time }) "
            "RETURN c.title AS title"
        )
        result = tx.run(query, url=url, crawl_time=crawl_time)
        res = [row["title"] for row in result]
        if res == None:
            return None
        else:
            return res

    @staticmethod
    def _create_content_and_match_with_info(tx, url, title, date, n_view, n_comment, good, warm, sad, angry, want, crawl_time, category, rank, creator, platform):
        query = (
            "CREATE (c:Content { url: $url, title: $title, date: $date, n_view: $n_view, n_comment: $n_comment, good: $good, warm: $warm, sad: $sad, angry: $angry, want: $want }) "
            "WITH c "
            "MATCH (ct:CrawlTime) WHERE ct.time = $crawl_time "
            "MATCH (t:Type { name: $category, platform: $platform  }) "
            "MATCH (r:Rank { n: $rank, platform: $platform }) "
            "MATCH (cr:Creator) WHERE cr.name = $creator AND cr.type = 'press' "
            "CREATE (c)-[:CRAWLED_AT]->(ct) "
            "CREATE (c)<-[:INCLUDES]-(t) "
            "CREATE (c)<-[:RANKS]-(r) "
            "CREATE (c)<-[:MAKES]-(cr) "
        )
        result = tx.run(query, url=url, title=title, date=date, n_view=n_view, n_comment=n_comment,\
            good=good, warm=warm, sad=sad, angry=angry, want=want, \
                crawl_time=crawl_time, category=category, rank=rank, creator=creator, platform=platform)

    @staticmethod
    def _match_content_with_info(tx, url, crawl_time, category, rank, platform):
        query = (
            "MATCH (c: Content { url: $url })-[:CRAWLED_AT]->(ct: CrawlTime { time: $crawl_time }) "
            "MATCH (t:Type { name: $category, platform: $platform }) "
            "MATCH (r:Rank { n: $rank, platform: $platform }) "
            "CREATE (c)<-[:INCLUDES]-(t) "
            "CREATE (c)<-[:RANKS]-(r) "
        )
        result = tx.run(query, url=url, crawl_time=crawl_time, category=category, rank=rank, platform=platform)

        
    @staticmethod
    def _create_content_test(tx, url, title, date, n_view, n_comment, good, warm, sad, angry, want, crawl_time, category, rank, creator, platform):
        query = (
            "CREATE (c:Content { url: $url, title: $title, date: $date, n_view: $n_view, n_comment: $n_comment, good: $good, warm: $warm, sad: $sad, angry: $angry, want: $want, crawl_time: $crawl_time, category: $category, rank: $rank, creator: $creator, platform: $platform }) "
        )
        result = tx.run(query, url=url, title=title, date=date, n_view=n_view, n_comment=n_comment,\
            good=good, warm=warm, sad=sad, angry=angry, want=want, \
                crawl_time=crawl_time, category=category, rank=rank, creator=creator, platform=platform)



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
    with open("/home/capje/tmp_data/naver_data_B_pre.json",'r') as f:
        json_data = json.load(f)
    
    for value in tqdm(json_data):
        app.create_crawltime(value['crawl_time'])
        app.create_creator_and_match_platform(value['press'], platform)

        app.create_content_and_match_with_info(value['url'], value['title'], value['date'], value['n_view'], value['n_comment'], \
            value['n_reaction_good'], value['n_reaction_warm'], value['n_reaction_sad'], \
            value['n_reaction_angry'], value['n_reaction_want'], \
            value['crawl_time'], 'bestview', value['rank'], value['press'], platform)

        for elem in value['keyword']:
            app.create_keyword(elem)
            app.match_content_and_keyword(elem, value['url'], value['title'], value['date'], value['n_view'], value['n_comment'], \
                value['n_reaction_good'], value['n_reaction_warm'], value['n_reaction_sad'], \
                value['n_reaction_angry'], value['n_reaction_want'])
                
    app.close()
