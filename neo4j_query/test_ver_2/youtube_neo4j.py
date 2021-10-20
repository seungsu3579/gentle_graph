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

    def create_creator_and_match_platform(self, creator, creator_url, platform):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_creator, creator, creator_url)
            if len(result) == 0:
                session.write_transaction(self._create_creator, creator, creator_url)
            result = session.read_transaction(self._find_edge_with_creator_and_platform, creator, creator_url, platform)
            if len(result) == 0:
                session.write_transaction(self._match_creator_and_platform, creator, creator_url, platform)

    def create_content_and_match_with_info(self, url, title, date, n_view, n_comment, good, bad, hashtags, description, crawl_time, category, rank, creator, creator_url, platform):
        with self.driver.session() as session:
            # 해당 crawl_time에 content가 존재하는지 확인
            result = session.read_transaction(self._find_and_return_content, url, crawl_time)
            if len(result) == 0:
                result = session.write_transaction(
                    self._create_content_and_match_with_info, url, title, date, n_view, n_comment, good, bad, hashtags, description, crawl_time, category, rank, creator, creator_url, platform
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

    # 모든 항목 content의 property로 넣기
    def create_content_test(self, url, title, date, n_view, n_comment, good, bad, hashtags, description, crawl_time, category, rank, creator, creator_url, platform):
        with self.driver.session() as session:
            session.write_transaction(
                self._create_content_test, url, title, date, n_view, n_comment, good, bad, hashtags, description, crawl_time, category, rank, creator, creator_url, platform
            )

    # keyword connection
    def match_keywords(self, keyword1, keyword2):
        with self.driver.session() as session:
            session.write_transaction(
                self._match_keywords, keyword1, keyword2
            )

    @staticmethod
    def _match_keywords(tx, keyword1, keyword2):
        query = (
            "MATCH (k1:Keyword { name: $keyword1 }) "
            "MATCH (k2:Keyword { name: $keyword2 }) "
            "CREATE (k1)-[:CONNECTS]->(k2)"
        )

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
    def _find_and_return_creator(tx, creator, creator_url):
        query = (
            "MATCH (c:Creator { name: $creator, creator_url: $creator_url }) "
            "RETURN c.name AS name "
        )
        result = tx.run(query, creator=creator, creator_url=creator_url)
        res = [row["name"] for row in result]
        if res == None:
            return None
        else:
            return res

    @staticmethod
    def _find_edge_with_creator_and_platform(tx, creator, creator_url, platform):
        query = (
            "MATCH (c:Creator { name: $creator, creator_url: $creator_url })-[:USES]->(p:Platform { name: $platform }) "
            "RETURN c.name AS name "
        )
        result = tx.run(query, creator=creator, creator_url=creator_url, platform=platform)
        res = [row["name"] for row in result]
        if res == None:
            return None
        else:
            return res

    @staticmethod
    def _create_creator(tx, creator, creator_url):
        query = (
            "CREATE (c: Creator { name: $creator, creator_url: $creator_url, type: 'channel' }) "
            "return c"
        )
        result = tx.run(query, creator=creator, creator_url=creator_url)

    @staticmethod
    def _match_creator_and_platform(tx, creator, creator_url, platform):
        query = (
            "MATCH (c: Creator { name: $creator, creator_url: $creator_url }) "
            "MATCH (p: Platform { name: $platform }) "
            "CREATE (c)-[:USES]->(p) "
        )
        tx.run(query, creator=creator, creator_url=creator_url, platform=platform)
    
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
    def _match_content_and_keyword(tx, keyword, url, crawl_time):
        query = (
            "MATCH (c: Content { url: $url })-[:CRAWLED_AT]->(ct: CrawlTime { time: $crawl_time}) "
            "WITH c "
            "MATCH (k: Keyword { name: $keyword}) "
            "CREATE (c)-[:EXTRACTS]->(k)"
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
    def _create_content_and_match_with_info(tx, url, title, date, n_view, n_comment, good, bad, hashtags, description, crawl_time, category, rank, creator, creator_url, platform):
        query = (
            "CREATE (c:Content { url: $url, title: $title, date: $date, n_view: $n_view, n_comment: $n_comment, good: $good, bad: $bad, hashtags: $hashtags, description: $description }) "
            "WITH c "
            "MATCH (ct:CrawlTime { time: $crawl_time }) "
            "MATCH (t:Type { name: $category, platform: $platform }) "
            "MATCH (r:Rank { n: $rank, platform: $platform }) "
            "MATCH (cr:Creator { name: $creator, creator_url: $creator_url }) "
            "CREATE (c)-[:CRAWLED_AT]->(ct) "
            "CREATE (c)<-[:INCLUDES]-(t) "
            "CREATE (c)<-[:RANKS]-(r) "
            "CREATE (c)<-[:MAKES]-(cr) "
        )
        result = tx.run(query, url=url, title=title, date=date, n_view=n_view, n_comment=n_comment,\
                good=good, bad=bad, hashtags=hashtags, description=description, crawl_time=crawl_time, \
                category=category, rank=rank, creator=creator, creator_url=creator_url, platform=platform)

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
    def _create_content_test(tx, url, title, date, n_view, n_comment, good, bad, hashtags, description, crawl_time, category, rank, creator, creator_url, platform):
        query = (
            "CREATE (c:Content { url: $url, title: $title, date: $date, n_view: $n_view, n_comment: $n_comment, good: $good, bad: $bad, hashtags: $hashtags, description: $description, crawl_time: $crawl_time, category: $category, rank: $rank, creator: $creator, creator_url: $creator_url, platform: $platform }) "
        )
        result = tx.run(query, url=url, title=title, date=date, n_view=n_view, n_comment=n_comment,\
            good=good, bad=bad, hashtags=hashtags, description=description, \
                crawl_time=crawl_time, category=category, rank=rank, creator=creator, creator_url=creator_url, platform=platform)

if __name__ == "__main__":
    # Aura queries use an encrypted connection using the "neo4j+s" URI scheme
    bolt_url = "bolt://localhost:7687"
    user = "neo4j"
    password = "neo4j123"
    app = App(bolt_url, user, password)
    #data part
    platform = 'Youtube'
    import os
    print(os.getcwd())
    import json
    with open("/home/capje/tmp_data/youtube_data_pre.json",'r') as f:
        json_data = json.load(f)
    
    for value in tqdm(json_data):
        app.create_crawltime(value['crawl_time'])
        app.create_creator_and_match_platform(value['creator'], value['creator_url'], platform)

        app.create_content_and_match_with_info(value['url'], value['title'], value['date'], value['n_views'], value['n_comment'], \
            value['n_reaction_good'], value['n_reaction_bad'], value['hashtags'], value['description'], \
            value['crawl_time'], 'korea_popular', value['rank'], value['creator'], value['creator_url'], platform)

        for elem in value['keyword']:
            app.create_keyword(elem)
            app.match_content_and_keyword(elem, value['url'], value['title'], value['date'], value['n_views'], value['n_comment'], \
                value['n_reaction_good'], value['n_reaction_bad'])
        
    app.close()
