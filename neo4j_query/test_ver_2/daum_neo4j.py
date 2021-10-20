import re
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

    def create_content_and_match_with_info(self, url, title, date, n_comment, recommend, like, impress, angry, sad, crawl_time, category, rank, creator, platform):
        with self.driver.session() as session:
            # 해당 crawl_time에 content가 존재하는지 확인
            result = session.read_transaction(self._find_and_return_content, url, crawl_time)
            if len(result) == 0:
                result = session.write_transaction(
                    self._create_content_and_match_with_info, url, title, date, n_comment, recommend, like, impress, angry, sad, crawl_time, category, rank, creator, platform
                )
            else:
                # 해당 category에 content가 존재하는지 확인
                result = session.read_transaction(self._find_and_return_category, url, crawl_time, category, platform)
                if len(result) == 0:
                    result = session.write_transaction(
                        self._match_content_with_info, url, crawl_time, category, rank, platform
                    )

    def create_content_and_match_with_info_age(self, url, title, date, n_comment, recommend, like, impress, angry, sad, crawl_time, category, age, sex, rank, creator, platform):
        with self.driver.session() as session:
            # 해당 crawl_time에 content가 존재하는지 확인
            result = session.read_transaction(self._find_and_return_content, url, crawl_time)
            if len(result) == 0:
                result = session.write_transaction(
                    self._create_content_and_match_with_info_age, url, title, date, n_comment, recommend, like, impress, angry, sad, crawl_time, category, age, sex, rank, creator, platform
                )
            else:
                # 해당 category에 content가 존재하는지 확인
                result = session.read_transaction(self._find_and_return_category_age, url, crawl_time, category, age, sex, platform)
                if len(result) == 0:
                    result = session.write_transaction(
                        self._match_content_with_info_age, url, crawl_time, category, age, sex, rank, platform
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
    def create_content_test(self, url, title, date, n_comment, recommend, like, impress, angry, sad):
        with self.driver.session() as session:
            session.write_transaction(
                self._create_content_test, url, title, date, n_comment, recommend, like, impress, angry, sad
            )

    # 모든 항목 content의 property로 넣기
    def create_content_age_test(self, url, title, date, n_comment, recommend, like, impress, angry, sad, crawl_time, category, age, sex, rank, creator, platform):
        with self.driver.session() as session:
            session.write_transaction(
                self._create_content_age_test, url, title, date, n_comment, recommend, like, impress, angry, sad, crawl_time, category, age, sex, rank, creator, platform
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
    def _find_and_return_category(tx, url, crawl_time, category, platform):
        query = (
            "MATCH (c: Content { url: $url })-[:CRAWLED_AT]->(cr: CrawlTime { time: $crawl_time }) "
            "WITH c "
            "MATCH (c)<-[:INCLUDES]-(t:Type { name: $category, platform: $platform  }) "
            "RETURN t.name AS name"
        )
        result = tx.run(query, url=url, crawl_time=crawl_time, category=category, platform=platform)
        res = [row["name"] for row in result]
        if res == None:
            return None
        else:
            return res

    @staticmethod
    def _find_and_return_category_age(tx, url, crawl_time, category, age, sex, platform):
        query = (
            "MATCH (c: Content { url: $url })-[:CRAWLED_AT]->(cr: CrawlTime { time: $crawl_time }) "
            "WITH c "
            "MATCH (c)<-[:INCLUDES]-(t:Type { name: $category, age: $age, sex: $sex, platform: $platform  }) "
            "RETURN t.name AS name"
        )
        result = tx.run(query, url=url, crawl_time=crawl_time, category=category, age=age, sex=sex, platform=platform)
        res = [row["name"] for row in result]
        if res == None:
            return None
        else:
            return res

    @staticmethod
    def _create_content_and_match_with_info(tx, url, title, date, n_comment, recommend, like, impress, angry, sad, crawl_time, category, rank, creator, platform):
        query = (
            "CREATE (c:Content { url: $url, title: $title, date: $date, n_comment: $n_comment, recommend: $recommend, like: $like, impress: $impress, angry: $angry, sad: $sad }) "
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
        result = tx.run(query, url=url, title=title, date=date, n_comment=n_comment,\
            recommend=recommend, like=like, impress=impress, angry=angry, sad=sad, \
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
        result = tx.run(query, url=url, crawl_time=crawl_time, \
            category=category, rank=rank, platform=platform)

    @staticmethod
    def _create_content_and_match_with_info_age(tx, url, title, date, n_comment, recommend, like, impress, angry, sad, crawl_time, category, age, sex, rank, creator, platform):
        query = (
            "CREATE (c:Content { url: $url, title: $title, date: $date, n_comment: $n_comment, recommend: $recommend, like: $like, impress: $impress, angry: $angry, sad: $sad }) "
            "WITH c "
            "MATCH (ct:CrawlTime) WHERE ct.time = $crawl_time "
            "MATCH (t:Type) WHERE t.name = $category AND t.age = $age AND t.sex = $sex "
            "MATCH (r:Rank { n: $rank, platform: $platform}) "
            "MATCH (cr:Creator) WHERE cr.name = $creator AND cr.type = 'press' "
            "CREATE (c)-[:CRAWLED_AT]->(ct) "
            "CREATE (c)<-[:INCLUDES]-(t) "
            "CREATE (c)<-[:RANKS]-(r) "
            "CREATE (c)<-[:MAKES]-(cr) "
        )
        result = tx.run(query, url=url, title=title, date=date, n_comment=n_comment,\
            recommend=recommend, like=like, impress=impress, angry=angry, sad=sad, \
                crawl_time=crawl_time, category=category, age=age, sex=sex, \
                    rank=rank, creator=creator, platform=platform)
        try:
            # 일부만 작성
            return [{"c": row["c"]["title"], "com": row["com"]["n"], \
                "react": row["react"], "ct": row["ct"]["time"]} for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    @staticmethod
    def _match_content_with_info_age(tx, url, crawl_time, category, age, sex, rank, platform):
        query = (
            "MATCH (c: Content { url: $url })-[:CRAWLED_AT]->(ct: CrawlTime { time: $crawl_time }) "
            "MATCH (t:Type { name: $category, platform: $platform, age: $age, sex: $sex }) "
            "MATCH (r:Rank { n: $rank, platform: $platform }) "
            "CREATE (c)<-[:INCLUDES]-(t) "
            "CREATE (c)<-[:RANKS]-(r) "
        )
        result = tx.run(query, url=url, crawl_time=crawl_time, category=category,\
            age=age, sex=sex, rank=rank, platform=platform)

    @staticmethod
    def _create_content_test(tx, url, title, date, n_view, n_comment, recommend, like, impress, angry, sad, crawl_time, category, rank, creator, platform):
        query = (
            "CREATE (c:Content { url: $url, title: $title, date: $date, n_view: $n_view, n_comment: $n_comment, recommend: $recommend, like: $like, impress: $impress, angry: $angry, sad: $sad, crawl_time: $crawl_time, category: $category, rank: $rank, creator: $creator, platform: $platform }) "
        )
        result = tx.run(query, url=url, title=title, date=date, n_view=n_view, n_comment=n_comment,\
            recommend=recommend, like=like, impress=impress, angry=angry, sad=sad, \
                crawl_time=crawl_time, category=category, rank=rank, creator=creator, platform=platform)

    @staticmethod
    def _create_content_age_test(tx, url, title, date, n_view, n_comment, recommend, like, impress, angry, sad, crawl_time, category, age, sex, rank, creator, platform):
        query = (
            "CREATE (c:Content { url: $url, title: $title, date: $date, n_view: $n_view, n_comment: $n_comment, recommend: $recommend, like: $like, impress: $impress, angry: $angry, sad: $sad, crawl_time: $crawl_time, category: $category, age: $age, sex: $sex, rank: $rank, creator: $creator, platform: $platform }) "
        )
        result = tx.run(query, url=url, title=title, date=date, n_view=n_view, n_comment=n_comment,\
            recommend=recommend, like=like, impress=impress, angry=angry, sad=sad, \
                crawl_time=crawl_time, category=category, age=age, sex=sex, rank=rank, creator=creator, platform=platform)

if __name__ == "__main__":
    # Aura queries use an encrypted connection using the "neo4j+s" URI scheme
    bolt_url = "bolt://localhost:7687"
    user = "neo4j"
    password = "neo4j123"
    app = App(bolt_url, user, password)
    #data part
    platform = 'Daum'
    import os
    print(os.getcwd())
    import json
    with open("/home/capje/tmp_data/Daum_ranking_news_20210505_173102_pre.json",'r') as f:
        json_data = json.load(f)
    
    for value in tqdm(json_data):
        app.create_crawltime(value['crawl_time'])
        app.create_creator_and_match_platform(value['press'], platform)

        if value['category'] == 'age':
            app.create_content_and_match_with_info_classified_by_age(value['url'], value['title'], value['date'], value['n_comment'], \
                value['n_reaction_recommend'], value['n_reaction_like'], value['n_reaction_impress'], \
                value['n_reaction_angry'], value['n_reaction_sad'], value['crawl_time'], \
                value['category'], value['age'], value['sex'], value['rank'], value['press'], platform)

        else:
            app.create_content_and_match_with_info(value['url'], value['title'], value['date'], value['n_comment'], \
                value['n_reaction_recommend'], value['n_reaction_like'], value['n_reaction_impress'], \
                value['n_reaction_angry'], value['n_reaction_sad'], \
                value['crawl_time'], value['category'], value['rank'], value['press'], platform)

        for elem in value['keyword']:
            app.create_keyword(elem)
            app.match_content_and_keyword(elem, value['url'], value['crawl_time'])

    app.close()
