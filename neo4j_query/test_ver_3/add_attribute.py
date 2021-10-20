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

    def make_gds_graph(self, graphname, crawl_label):
        with self.driver.session() as session:
            session.write_transaction(self._make_gds_graph, graphname, crawl_label)

    def insert_pagerank(self, graphname):
        with self.driver.session() as session:
            session.write_transaction(self._insert_pagerank, graphname)

    def insert_betweeness(self, graphname):
        with self.driver.session() as session:
            session.write_transaction(self._insert_betweeness, graphname)

    def insert_closeness(self, graphname):
        with self.driver.session() as session:
            session.write_transaction(self._insert_closeness, graphname)

    def insert_degree(self, crawl_label):
        with self.driver.session() as session:
            session.write_transaction(self._insert_degree, crawl_label)

    def insert_clustering(self, graphname):
        with self.driver.session() as session:
            session.write_transaction(self._insert_clustering, graphname)

    def drop_gds_graph(self, graphname):
        with self.driver.session() as session:
            session.write_transaction(self._drop_gds_graph, graphname)
    
    def insert_reactions_naver(self, crawl_label):
        with self.driver.session() as session:
            session.write_transaction(self._insert_reactions_naver, crawl_label)

    def insert_reactions_daum(self, crawl_label):
        with self.driver.session() as session:
            session.write_transaction(self._insert_reactions_daum, crawl_label)

    def insert_reactions_youtube(self, crawl_label):
        with self.driver.session() as session:
            session.write_transaction(self._insert_reactions_youtube, crawl_label)

    @staticmethod
    def _make_gds_graph(tx, graphname, crawl_label):
        query = ("CALL gds.graph.create.cypher('"+graphname+"', 'MATCH (n:Content :"+crawl_label+") \
            RETURN id(n) AS id', 'MATCH (n:Content :"+crawl_label+")-[r:LINKS]->(m:Content :"+crawl_label+") \
                RETURN id(n) AS source, id(m) AS target, type(r) AS type, r.weight AS weight')")
        tx.run(query)

    @staticmethod
    def _insert_pagerank(tx, graphname):
        query = ("CALL gds.pageRank.write('"+graphname+"', {maxIterations: 20, dampingFactor: 0.85, \
            relationshipWeightProperty: 'weight', writeProperty: 'pagerank'}) YIELD nodePropertiesWritten, ranIterations")
        tx.run(query)

    @staticmethod
    def _insert_betweeness(tx, graphname):
        query = ("CALL gds.betweenness.write('"+graphname+"', {samplingSize: 2000, writeProperty:'betweeness'}) yield nodePropertiesWritten")
        tx.run(query)

    @staticmethod
    def _insert_closeness(tx, graphname):
        query = ("CALL gds.alpha.closeness.write('"+graphname+"', {writeProperty: 'closeness'}) YIELD nodes, writeProperty")
        tx.run(query)

    @staticmethod
    def _insert_clustering(tx, graphname):
        query = ("CALL gds.localClusteringCoefficient.write('"+graphname+"', {writeProperty: 'clustering'}) YIELD nodeCount, averageClusteringCoefficient")
        tx.run(query)

    @staticmethod
    def _insert_reactions_naver(tx, crawl_label):
        query = (
            "MATCH (u:Content :"+crawl_label+") "
            "WHERE u.platform = 'Naver' "
            "SET u.n_reactions = u.good + u.sad + u.angry + u.want + u.warm "
        )
        tx.run(query)
    @staticmethod
    def _insert_reactions_daum(tx, crawl_label):
        query = (
            "MATCH (u:Content :"+crawl_label+") "
            "WHERE u.platform = 'Daum' "
            "SET u.n_reactions = u.like + u.impress + u.angry + u.sad + u.recommend "
        )
        tx.run(query)
    @staticmethod
    def _insert_reactions_youtube(tx, crawl_label):
        query = (
            "MATCH (u:Content :"+crawl_label+") "
            "WHERE u.platform = 'Youtube' "
            "SET u.n_reactions = u.good + u.bad "
        )
        tx.run(query)
    
    @staticmethod
    def _insert_degree(tx, crawl_label):
        query = (
            "MATCH (u:Content :"+crawl_label+") "
            "SET u.degree = size((u)<-[:LINKS]-()) "
        )
        tx.run(query)
    
    @staticmethod
    def _drop_gds_graph(tx, graphname):
        query = ("CALL gds.graph.drop('"+graphname+"') YIELD graphName;")
        tx.run(query)

if __name__ == "__main__":
    # Aura queries use an encrypted connection using the "neo4j+s" URI scheme
    bolt_url = "bolt://localhost:7687"
    user = "neo4j"
    password = "neo4j123"
    app = App(bolt_url, user, password)
    name = "insert_graph"
    time = "T2021_05_12_05"
    for i in range(10, 21):
        time = 'T2021_05_24_'+str(i)
        print(time)
        app.make_gds_graph(name,time)
        app.insert_pagerank(name)
        app.insert_betweeness(name)
        app.insert_closeness(name)
        app.insert_degree(time)
        app.insert_clustering(name)
        app.drop_gds_graph(name)
    # app.insert_reactions_naver(time)
    # app.insert_reactions_daum(time)
    # app.insert_reactions_youtube(time)
    app.close()
