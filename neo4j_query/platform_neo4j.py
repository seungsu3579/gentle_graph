from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable
from tqdm import tqdm

from config import CONFIG
from config import TEST_CONFIG

class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    def create_platform(self, platform):
        with self.driver.session() as session:
            session.write_transaction(self._create_platform, platform)

    def create_type_and_match_platform(self, platform, type):
        with self.driver.session() as session:
            session.write_transaction(self._create_type_and_match_platform, platform, type)

    def create_rank(self, platform, rank):
        with self.driver.session() as session:
            session.write_transaction(self._create_rank, platform, rank)

    def match_type_and_rank(self, platform, type, rank):
        with self.driver.session() as session:
            session.write_transaction(self._match_type_and_rank, platform, type, rank)
    
    def match_age_type_and_rank(self, platform, type, age, sex, rank):
        with self.driver.session() as session:
            session.write_transaction(self._match_age_type_and_rank, platform, type, age, sex, rank)

    def create_age_type_and_match_platform(self, platform, type, age, sex):
        with self.driver.session() as session:
            session.write_transaction(self._create_age_type_and_match_platform, platform, type, age, sex)

    @staticmethod
    def _create_platform(tx, platform):
        query = (
            "CREATE (p:Platform { name: $platform }) "
        )
        tx.run(query, platform=platform)

    @staticmethod
    def _create_type_and_match_platform(tx, platform, type):
        query = (
            "MATCH (p:Platform { name: $platform})"
            "CREATE (t:Type { name: $type }) "
            "CREATE (p)-[:CLASSIFIES]->(t)"
        )
        tx.run(query, platform=platform, type=type)

    @staticmethod
    def _create_rank(tx, platform, rank):
        query = (
            "CREATE (r:Rank { n: $rank , platform: $platform}) "
        )
        tx.run(query, platform=platform, rank=rank)

    @staticmethod
    def _match_type_and_rank(tx, platform, type, rank):
        query = (
            "MATCH (t:Type { name: $type }) "
            "MATCH (r:Rank { n: $rank, platform: $platform }) "
            "CREATE (r)-[:RANKED_FROM]->(t)"
        )
        tx.run(query, platform=platform, type=type, rank=rank)

    @staticmethod
    def _match_age_type_and_rank(tx, platform, type, age, sex, rank):
        query = (
            "MATCH (t:Type { name: $type, age: $age, sex: $sex}) "
            "MATCH (r:Rank { n: $rank, platform: $platform}) "
            "CREATE (r)-[:RANKED_FROM]->(t)"
        )
        tx.run(query, platform=platform, type=type, age=age, sex=sex, rank=rank)

    @staticmethod
    def _create_age_type_and_match_platform(tx, platform, type, age, sex):
        query = (
            "MATCH (p:Platform { name: $platform})"
            "CREATE (t:Type { name: $type, age: $age, sex: $sex }) "
            "CREATE (p)-[:CLASSIFIES]->(t)"
        )
        tx.run(query, platform=platform, type=type, age=age, sex=sex)


if __name__ == "__main__":
    # Aura queries use an encrypted connection using the "neo4j+s" URI scheme

    import sys
    option = sys.argv[1]

    if option == "test":
        app = App(TEST_CONFIG["bolt_url"], TEST_CONFIG["user"], TEST_CONFIG["password"])
    
    elif option == "service":
        app = App(CONFIG["bolt_url"], CONFIG["user"], CONFIG["password"])


    #create platform
    platforms = ['Naver', 'Daum', 'Youtube']
    for platform in platforms:
        app.create_platform(platform)

    # Naver
    app.create_type_and_match_platform('Naver', 'bestview')

    for i in tqdm(range(21)):
        app.create_rank('Naver', i)
        app.match_type_and_rank('Naver', 'bestview', i)
    
    # Daum    
    categories = ["popular/news", "popular/entertain", "popular/sports", "kkomkkom/news", 
            "kkomkkom/entertain", "kkomkkom/sports", "bestreply"]
    ages = ['20대', '30대', '40대', '50대']

    for i in range(51):
        app.create_rank('Daum', i)

    for category in tqdm(categories):
        app.create_type_and_match_platform('Daum', category)
        for i in range(51):
            app.match_type_and_rank('Daum', category, i)
    
    for age in ages:
        app.create_age_type_and_match_platform('Daum', 'age', age, '여성')
        app.create_age_type_and_match_platform('Daum', 'age', age, '남성')
        for i in range(51):
            app.match_age_type_and_rank('Daum', 'age', age, '여성', i)
            app.match_age_type_and_rank('Daum', 'age', age, '남성', i)

    # YouTube
    app.create_type_and_match_platform('Youtube', 'korea_popular')

    for i in tqdm(range(200)):
        app.create_rank('Youtube', i)
        app.match_type_and_rank('Youtube', 'korea_popular', i)

    app.close()
