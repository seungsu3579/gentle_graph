from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable
from tqdm import tqdm

from sklearn.preprocessing import MinMaxScaler
import pandas as pd

import json
import os
class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    def insert_rank_and_platform(self):
        # content에 rank와 platform 정보 넣어줌
        with self.driver.session() as session:
            session.write_transaction(self._insert_rank_and_platform)

    def insert_edge_weight_pure(self, crawl_label):
        # rank 정보만을 가지고 edge에 weight를 넣어줌
        with self.driver.session() as session:
            session.write_transaction(self._insert_edge_weight_naver, crawl_label)
            session.write_transaction(self._insert_edge_weight_daum, crawl_label)
            session.write_transaction(self._insert_edge_weight_youtube, crawl_label)

    def ranked_by_pure_rank(self, crawl_label):
        # insert_edge_weight을 통해 넣은 데이터를 활용해 rank를 구함
        with self.driver.session() as session:
            tmp = session.read_transaction(self._ranked_by_pure_rank, crawl_label)
            result = []
            tmp_res = []
            idx = 1
            for elem in tmp:
                if elem[0] not in tmp_res:
                    tmp_res.append(elem[0])
                    result.append([idx,elem[0],elem[1],elem[2]])
                    idx += 1
            return result

    def ranked_by_degree(self, crawl_label):
        with self.driver.session() as session:
            tmp = session.read_transaction(self._ranked_by_degree, crawl_label)
            result = []
            tmp_res = []
            idx = 1
            for elem in tmp:
                if elem[0] not in tmp_res:
                    tmp_res.append(elem[0])
                    result.append([idx,elem[0],elem[1],elem[2]])
                    idx += 1
            return result

    def ranked_by_feature_importance(self, crawl_time):
        with self.driver.session() as session:
            scaler = MinMaxScaler()
            naver_all = session.read_transaction(self._get_content_naver, crawl_time)
            naver_keys = ['id','good','warm','sad','angry','want','n_comment', 'pagerank','betweeness','closeness','clustering', 'n_view','degree']
            naver_df = pd.DataFrame(naver_all, columns=naver_keys)
            indexes = naver_df['id']
            naver_df = naver_df.set_index('id')
            naver_df = pd.DataFrame(scaler.fit_transform(naver_df[naver_keys[1:]]), columns=naver_keys[1:], index = indexes)
            naver_df = naver_df.dropna(axis=0)
            imp = session.read_transaction(self._get_importance_naver)
            sum = 0
            for i in range(len(imp[0])):
                value = naver_df[naver_keys[i+1]] * imp[0][i]
                naver_df[naver_keys[i+1]] = value
                sum += value
            naver_df['sum'] = sum
            df = pd.DataFrame(naver_df['sum'], columns=['sum'], index=indexes)
            for i in range(len(naver_df)):
                session.write_transaction(self._insert_sum_of_weights_content, naver_df.index[i], naver_df.iloc[i, -1])
            
            daum_all = session.read_transaction(self._get_content_daum, crawl_time)
            daum_keys = ['id', 'like','impress','angry','sad','recommend', 'n_comment','pagerank','betweeness','closeness','clustering','degree']
            if (daum_all != []):
                daum_df = pd.DataFrame(daum_all, columns=daum_keys)
                indexes = daum_df['id']
                daum_df = daum_df.set_index('id')
                
                daum_df = pd.DataFrame(scaler.fit_transform(daum_df[daum_keys[1:]]), columns=daum_keys[1:], index = indexes)
                daum_df = daum_df.dropna(axis=0)
                imp = session.read_transaction(self._get_importance_daum)
                sum = 0
                for i in range(len(imp[0])):
                    value = daum_df[daum_keys[i+1]] * imp[0][i]
                    daum_df[daum_keys[i+1]] = value
                    sum += value
                daum_df['sum'] = sum
                temp = pd.DataFrame(daum_df['sum'], columns=['sum'], index=indexes)
                df = pd.concat([df, temp])
                for i in range(len(daum_df)):
                    session.write_transaction(self._insert_sum_of_weights_content, daum_df.index[i], daum_df.iloc[i, -1])
            
            youtube_all = session.read_transaction(self._get_content_youtube, crawl_time)
            youtube_keys = ['id', 'good','bad','n_comment','pagerank','betweeness','closeness','clustering','degree']
            youtube_df = pd.DataFrame(youtube_all, columns=youtube_keys)
            indexes = youtube_df['id']
            youtube_df = youtube_df.set_index('id')
            youtube_df = pd.DataFrame(scaler.fit_transform(youtube_df[youtube_keys[1:]]), columns=youtube_keys[1:], index = indexes)
            youtube_df = youtube_df.dropna(axis=0)
            imp = session.read_transaction(self._get_importance_youtube)
            sum = 0
            for i in range(len(imp[0])):
                value = youtube_df[youtube_keys[i+1]] * imp[0][i]
                youtube_df[youtube_keys[i+1]] = value
                sum += value
            youtube_df['sum'] = sum
            temp = pd.DataFrame(youtube_df['sum'], columns=['sum'], index=indexes)
            df = pd.concat([df, temp])
            for i in range(len(youtube_df)):
                session.write_transaction(self._insert_sum_of_weights_content, youtube_df.index[i], youtube_df.iloc[i, -1])

            tmp = session.write_transaction(self._ranked_by_weights_sum, crawl_time)
            result = []
            tmp_res = []
            idx = 1
            for elem in tmp:
                if elem[0] not in tmp_res:
                    tmp_res.append(elem[0])
                    result.append([idx,elem[0],elem[1],elem[2]])
                    idx += 1
            print(result)
            return result

    def keyword_info(self, crawl_label):
        keywords = self.ranked_by_feature_importance(crawl_label)
        return 
        keys_info = dict()
        with self.driver.session() as session:
            for key in keywords:
                key_info = dict()
                key_info['rank'] = key[0]
                key_info['name'] = key[1]
                key_info['weights'] = key[3]
                id = key[2]
                nkey = session.read_transaction(self._neighbor_keyword, id)
                key_info['n_key'] = nkey
                ncontent = session.read_transaction(self._neighbor_content, id)
                key_info['n_content'] = ncontent
                # ncon_info = dict()
                # for nc in ncontent:
                #     ncon_tmp = dict()
                #     ncon_tmp['id'] = nc[0]
                #     ncon_tmp['title'] = nc[1]
                #     ncon_tmp['url'] = nc[2]
                keys_info[key[2]] = key_info
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        with open(BASE_DIR+'/keyword_info_2.json','w',encoding='utf-8') as make_file:
            json.dump(keys_info, make_file, ensure_ascii=False, indent="\t")

    def keyword_info_fi(self, crawl_label):
        keywords = self.ranked_by_feature_importance(crawl_label)
        keys_info = dict()
        with self.driver.session() as session:
            for key in keywords:
                key_info = dict()
                key_info['rank'] = key[0]
                key_info['name'] = key[1]
                key_info['weights'] = key[3]
                id = key[2]
                nkey = session.read_transaction(self._neighbor_keyword_fi, id)
                k_list = []
                for k in nkey:
                    k_tmp = dict()
                    k_tmp['id'] = k[0]
                    k_tmp['name'] = k[1]
                    k_tmp['weights'] = k[2]
                    k_list.append(k_tmp)
                key_info['n_key'] = k_list
                ncontent = session.read_transaction(self._neighbor_content_fi, id, crawl_label)
                nc_list = []
                for nc in ncontent:
                    nc_tmp = dict()
                    nc_tmp['id'] = nc[0]
                    nc_tmp['title'] = nc[1]
                    nc_tmp['url'] = nc[2]
                    nc_tmp['weights'] = nc[3]
                    nc_list.append(nc_tmp)
                key_info['n_content'] = nc_list
                keys_info[key[2]] = key_info
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        filename = 'fi_'+crawl_label+'.json'
        with open(BASE_DIR+'/keyword_data/'+filename,'w',encoding='utf-8') as make_file:
            json.dump(keys_info, make_file, ensure_ascii=False, indent="\t")

    @staticmethod
    def _neighbor_keyword(tx, id):
        query = (
            "MATCH (n:Keyword) WHERE ID(n) = $id "
            "WITH n "
            "MATCH (n:Keyword)-[:CONNECTS]->(m) "
            "RETURN m.name AS name  "
        )
        res = tx.run(query, id=id)
        res = [i['name'] for i in res]
        return res
    @staticmethod
    def _neighbor_keyword_fi(tx, id):
        query = (
            "MATCH (n:Keyword) WHERE ID(n) = $id "
            "WITH n "
            "MATCH (n:Keyword)-[:CONNECTS]->(m) "
            "RETURN m.name AS name, ID(m) AS id, m.weights_sum AS w "
        )
        res = tx.run(query, id=id)
        res = [[i['id'],i['name'],i['w']] for i in res]
        return res

    @staticmethod
    def _neighbor_content(tx, id):
        query = (
            "MATCH (n:Keyword) WHERE ID(n) = $id "
            "WITH n "
            "MATCH (n:Keyword)<-[:EXTRACTS]-(m) "
            "RETURN ID(m) AS id, m.title AS title, m.url AS url "
        )
        res = tx.run(query, id=id)
        res = [[i['id'],i['title'], i['url']] for i in res]
        return res
    @staticmethod
    def _neighbor_content_fi(tx, id, crawl_label):
        query = (
            "MATCH (n:Keyword) WHERE ID(n) = $id "
            "WITH n "
            "MATCH (n:Keyword)<-[e:EXTRACTS]-(m:"+crawl_label+") "
            "RETURN ID(m) AS id, m.title AS title, m.url AS url, e.weights_sum as es "
            "ORDER BY es DESC "
            "LIMIT 7"
        )
        res = tx.run(query, id=id)
        res = [[i['id'],i['title'], i['url'], i['es']] for i in res]
        return res
    @staticmethod
    def _insert_rank_and_platform(tx):
        query = (
            "MATCH (u:Content)<-[:RANKS]-(r:Rank) "
            "set u.rank = r.n, u.platform = r.platform "
        )
        tx.run(query)

    @staticmethod
    def _insert_edge_weight_naver(tx, crawl_label):
        query = (
            "MATCH (n:Content :"+crawl_label+" {platform: 'Naver'})-[e :EXTRACTS]->() "
            "with n, e "
            "set e.weights = toFloat(21-n.rank)/20 "
        )
        tx.run(query)

    @staticmethod
    def _insert_edge_weight_daum(tx, crawl_label):
        query = (
            "MATCH (n:Content :"+crawl_label+" {platform: 'Daum'})-[e :EXTRACTS]->() "
            "with n, e "
            "set e.weights = toFloat(51-n.rank)/50 "
        )
        tx.run(query)

    @staticmethod
    def _insert_edge_weight_youtube(tx, crawl_label):
        query = (
            "MATCH (n:Content :"+crawl_label+" {platform: 'Youtube'})-[e :EXTRACTS]->() "
            "with n, e "
            "set e.weights = toFloat(201-n.rank)/200 "
        )
        tx.run(query)

    @staticmethod
    def _ranked_by_pure_rank(tx, crawl_label):
        query = (
            "MATCH (n:Keyword :"+crawl_label+")<-[e:EXTRACTS]-() "
            "RETURN n.name AS name, ID(n) AS id, sum(e.weights) AS sum_rank "
            "order by sum_rank desc "
            "LIMIT 50 "
        )
        result = tx.run(query)
        res = [[row["name"],row['id'],row['sum_rank']] for row in result]
        return res

    @staticmethod
    def _ranked_by_degree(tx, crawl_label):
        query = (
            "MATCH (u:Keyword :"+crawl_label+") "
            "RETURN u.name AS name, ID(u) AS id, size((u)<-[:EXTRACTS]-()) AS in_degree "
            "order by in_degree desc "
            "LIMIT 50 "
        )
        result = tx.run(query)
        res = [[row["name"],row["id"],row['in_degree']] for row in result]
        return res

    @staticmethod
    def _find_keyword(tx, crawl_time):
        query = (
            "MATCH (k:Keyword:"+crawl_time+") "
            "RETURN k.name AS name"
        )
        result = tx.run(query)
        return [row['name'] for row in result]

    @staticmethod
    def _find_content_by_keyword(tx, keyword, crawl_time):
        query = (
            "MATCH (k:Keyword:"+crawl_time+" { name: $keyword })<-[:EXTRACTS]-(c:Content:"+crawl_time+") "
            "RETURN ID(c) AS id"
        )
        result = tx.run(query, keyword=keyword)
        return [ row['id'] for row in result]
    
    @staticmethod
    def _find_content_by_platform(tx, platform, crawl_time):
        query = (
            "MATCH (c:Content:"+crawl_time+")<-[:INCLUDES]-(t:Type)<-[:CLASSIFIES]-(p:Platform { name: $platform })"
            "RETURN ID(c) AS id, c "
        )
        result = tx.run(query, platform=platform)
        return [[row['id'], row['c']] for row in result]

    @staticmethod
    def _get_content_naver(tx, crawl_time):
        query = (
            "MATCH (c:Content:"+crawl_time+")<-[:INCLUDES]-(t:Type)<-[:CLASSIFIES]-(p:Platform { name: 'Naver' })"
            "RETURN ID(c) AS id, c.good AS good, c.warm AS warm, c.sad AS sad, c.angry AS angry, c.want AS want, "
            "c.n_comment AS n_comment, c.pagerank AS pagerank, c.betweeness AS betweeness, c.closeness AS closeness, c.clustering AS clustering, "
            "c.n_view AS n_view, c.degree AS degree"
        )
        result = tx.run(query)
        return [[row['id'], row['good'], row['warm'], row['sad'], row['angry'], row['want'], 
                row['n_comment'], row['pagerank'],  row['betweeness'], row['closeness'], row['clustering'], row['n_view'], row['degree']]  for row in result]

    @staticmethod
    def _get_content_daum(tx, crawl_time):
        query = (
            "MATCH (c:Content:"+crawl_time+")<-[:INCLUDES]-(t:Type)<-[:CLASSIFIES]-(p:Platform { name: 'Daum' })"
            "RETURN ID(c) AS id, c.like AS like, c.impress AS impress, c.angry AS angry, c.sad AS sad, c.recommend AS recommend, "
            "c.n_comment AS n_comment, c.pagerank AS pagerank, c.betweeness AS betweeness, c.closeness AS closeness, c.clustering AS clustering, c.degree AS degree"
        )
        result = tx.run(query)
        return [[row['id'], row['like'], row['impress'], row['angry'], row['sad'], row['recommend'], 
                row['n_comment'], row['pagerank'], row['betweeness'], row['closeness'], row['clustering'], row['degree']] for row in result]

    @staticmethod
    def _get_content_youtube(tx, crawl_time):
        query = (
            "MATCH (c:Content:"+crawl_time+")<-[:INCLUDES]-(t:Type)<-[:CLASSIFIES]-(p:Platform { name: 'Youtube' })"
            "RETURN ID(c) AS id,  c.good AS good, c.bad AS bad, c.n_comment AS n_comment, "
            "c.pagerank AS pagerank, c.betweeness AS betweeness, c.closeness AS closeness, c.clustering AS clustering, c.degree AS degree"
        )
        result = tx.run(query)
        return [[row['id'], row['good'], row['bad'], row['n_comment'], 
                row['pagerank'], row['betweeness'], row['closeness'], row['clustering'], row['degree']] for row in result]

    @staticmethod
    def _get_importance_naver(tx):
        query = (
            "MATCH (c : Importance { platform: 'Naver' }) "
            "RETURN c.good AS good, c.warm AS warm, c.sad AS sad, c.angry AS angry, c.want AS want, "
            "c.n_comment AS n_comment, c.pagerank AS pagerank, c.betweeness AS betweeness, c.closeness AS closeness, c.clustering AS clustering, "
            "c.n_view AS n_view, c.degree AS degree"
        )
        result = tx.run(query)
        return [[row['good'], row['warm'], row['sad'], row['angry'], row['want'], 
                row['n_comment'], row['pagerank'],  row['betweeness'], row['closeness'], row['clustering'], row['n_view'], row['degree']]  for row in result]

    @staticmethod
    def _get_importance_daum(tx):
        query = (
            "MATCH (c : Importance { platform: 'Daum' }) "
            "RETURN c.like AS like, c.impress AS impress, c.angry AS angry, c.sad AS sad, c.recommend AS recommend, "
            "c.n_comment AS n_comment, c.pagerank AS pagerank, c.betweeness AS betweeness, c.closeness AS closeness, c.clustering AS clustering, c.degree AS degree"
        )
        result = tx.run(query)
        return [[ row['like'], row['impress'], row['angry'], row['sad'], row['recommend'], 
                row['n_comment'], row['pagerank'], row['betweeness'], row['closeness'], row['clustering'], row['degree']] for row in result]

    @staticmethod
    def _get_importance_youtube(tx):
        query = (
            "MATCH (c : Importance { platform: 'Youtube' }) "
            "RETURN  c.good AS good, c.bad AS bad, c.n_comment AS n_comment, "
            "c.pagerank AS pagerank, c.betweeness AS betweeness, c.closeness AS closeness, c.clustering AS clustering, c.degree AS degree"
        )
        result = tx.run(query)
        return [[row['good'], row['bad'], row['n_comment'],
                row['pagerank'], row['betweeness'], row['closeness'], row['clustering'], row['degree']] for row in result]

    @staticmethod
    def _insert_sum_of_weights_content(tx, id, sum):
        query = (
            "MATCH (c:Content) WHERE ID(c) = $id "
            "WITH c "
            "MATCH (c)-[e:EXTRACTS]->() "
            "set e.weights_sum = $sum"
        )
        tx.run(query, id=int(id), sum=sum)

    @staticmethod
    def _ranked_by_weights_sum(tx, crawl_time):
        query = (
            "MATCH (n:Keyword :"+crawl_time+")<-[e:EXTRACTS]-() "
            "WITH n, sum(e.weights_sum) AS weights_sum "
            "SET n.weights_sum = weights_sum "
            "RETURN n.name AS name, ID(n) AS id, n.weights_sum AS sum_rank "
            "order by sum_rank desc "
            "LIMIT 50 "
        )
        result = tx.run(query)
        res = [[row["name"],row['id'],row['sum_rank']] for row in result]
        return res

if __name__ == "__main__":
    # Aura queries use an encrypted connection using the "neo4j+s" URI scheme
    bolt_url = "bolt://localhost:7687"
    user = "neo4j"
    password = "neo4j123"
    app = App(bolt_url, user, password)
    app.insert_rank_and_platform()
    time = "T2021_05_28_13"
    # app.insert_edge_weight_pure(time)
    # res = app.ranked_by_pure_rank(time)
    # print(res)
    # res2 = app.ranked_by_degree(time)
    # print(res2)
    #app.insert_edge_weight(time)
    app.keyword_info_fi(time)
    app.close()