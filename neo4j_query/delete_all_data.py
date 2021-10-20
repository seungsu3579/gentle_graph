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

    def delete_all_data(self):
        with self.driver.session() as session:
            session.write_transaction(self._delete_content)
            session.write_transaction(self._delete_crawltime)
            session.write_transaction(self._delete_creator)
            session.write_transaction(self._delete_keyword)
            session.write_transaction(self._delete_platform)
            session.write_transaction(self._delete_rank)
            session.write_transaction(self._delete_type)

    @staticmethod
    def _delete_content(tx):
        query = (
            "MATCH (c:Content) DETACH DELETE c "
        )
        tx.run(query)

    @staticmethod
    def _delete_crawltime(tx):
        query = (
            "MATCH (c:CrawlTime) DETACH DELETE c "
        )
        tx.run(query)

    @staticmethod
    def _delete_creator(tx):
        query = (
            "MATCH (c:Creator) DETACH DELETE c "
        )
        tx.run(query)

    @staticmethod
    def _delete_keyword(tx):
        query = (
            "MATCH (c:Keyword) DETACH DELETE c "
        )
        tx.run(query)

    @staticmethod
    def _delete_platform(tx):
        query = (
            "MATCH (c:Platform) DETACH DELETE c "
        )
        tx.run(query)

    @staticmethod
    def _delete_rank(tx):
        query = (
            "MATCH (c:Rank) DETACH DELETE c "
        )
        tx.run(query)

    @staticmethod
    def _delete_type(tx):
        query = (
            "MATCH (c:Type) DETACH DELETE c "
        )
        tx.run(query)

if __name__ == "__main__":
    # Aura queries use an encrypted connection using the "neo4j+s" URI scheme

    import sys
    option = sys.argv[1]

    if option == "test":
        app = App(TEST_CONFIG["bolt_url"], TEST_CONFIG["user"], TEST_CONFIG["password"])
    
    elif option == "service":
        app = App(CONFIG["bolt_url"], CONFIG["user"], CONFIG["password"])

    for i in tqdm(range(1)):
        app.delete_all_data()

    app.close()
