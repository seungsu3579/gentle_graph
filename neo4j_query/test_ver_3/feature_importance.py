from itertools import permutations

from numpy.core.fromnumeric import var
from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable
from tqdm import tqdm

from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import time
import numpy as np
import pandas as pd
import datetime

class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    def randomforest(self, data:list, feature:list):
        scaler = StandardScaler()
        print('data size: ', len(data))
        forest = RandomForestClassifier(random_state=13, n_estimators=500, max_depth=15)

        df = pd.DataFrame(data, columns=feature)
        df = df.dropna()
        X = scaler.fit_transform(df.iloc[:, :-1])
        y = df.iloc[:, -1]
        X, X_test, y, y_test = train_test_split(X, y, stratify=y, random_state=13, test_size=0.15)
        forest.fit(X, y)

        print("RF accuracy: %0.3f" % forest.score(X, y))
        # print("RF test accuracy: %0.3f" % forest.score(X_test, y_test))

        start_time = time.time()
        importances = forest.feature_importances_
        # elapsed_time = time.time() - start_time

        # print(f"Elapsed time to compute the importances: "
        #     f"{elapsed_time:.3f} seconds")
        
        forest_importances = pd.Series(importances, index=feature[:-1])
        # print(forest_importances)
        return forest_importances     

    def load_data(self, platform:str):
        df, features = [], []
        with self.driver.session() as session:
            data = session.read_transaction(self._load_data, platform)
            for row in data:
                try:    
                    if platform == 'Naver':
                        feature = ['good','warm','sad','angry','want','n_comment', 'pagerank','betweeness','closeness','clustering', 'n_view','degree', 'rank']
                        features = [row[0]['good'], row[0]['warm'], row[0]['sad'], row[0]['angry'], row[0]['want'], 
                            row[0]['n_comment'], row[0]['pagerank'],  row[0]['betweeness'], row[0]['closeness'], row[0]['clustering'], row[0]['n_view'], row[0]['degree'], round(row[1])]

                    elif platform == 'Daum':
                        feature = ['like','impress','angry','sad','recommend', 'n_comment','pagerank','betweeness','closeness','clustering', 'degree', 'rank']
                        features = [row[0]['like'], row[0]['impress'], row[0]['angry'], row[0]['sad'], row[0]['recommend'], 
                            row[0]['n_comment'], row[0]['pagerank'], row[0]['betweeness'], row[0]['closeness'], row[0]['clustering'], row[0]['degree'], round(row[1])]
                    
                    elif platform == 'Youtube':
                        feature = ['n_view', 'bad','n_comment','pagerank','betweeness','closeness','clustering', 'degree', 'rank'] # good, n_view
                        features = [row[0]['n_view'], row[0]['bad'], row[0]['n_comment'], 
                            row[0]['pagerank'], row[0]['betweeness'], row[0]['closeness'], row[0]['clustering'], row[0]['degree'], round(row[1])]
                    
                    df.append(features)
                except:
                    pass

        return df,feature
    
    def insert_feature_importance(self, rf:dict, time:str):
        with self.driver.session() as session:
            for platform in rf.keys():
                session.write_transaction(self._create_importance_frame, platform)
                session.write_transaction(self._insert_feature_importance, platform, 'time', time)
                for item in rf[platform].items():
                    session.write_transaction(self._insert_feature_importance, platform, item[0], item[1])

    @staticmethod
    def _load_data(tx, platform):
        query = (
            "MATCH (c:Content)<-[:RANKS]-(r:Rank { platform: $platform }) "
            "RETURN c, r.n AS rank "
        )
        result = tx.run(query, platform=platform)
        return [[row['c'], row['rank']] for row in result] 

    @staticmethod
    def _create_importance_frame(tx, platform):
        query = (
            "MERGE (i:Importance { platform: $platform }) "
            "RETURN i "
        )
        tx.run(query, platform=platform)

    @staticmethod
    def _insert_feature_importance(tx, platform, value1, value2):
        query = (
            "MATCH (i:Importance { platform: $platform }) "
            "SET i."+value1+" = $value2 "
            "RETURN i"
        )
        tx.run(query, platform=platform, value2=value2)

if __name__ == "__main__":
    # Aura queries use an encrypted connection using the "neo4j+s" URI scheme
    bolt_url = "bolt://localhost:7687"
    user = "neo4j"
    password = "neo4j123"
    app = App(bolt_url, user, password)
    
    output = dict()
    platforms = ['Naver', 'Daum', 'Youtube']
    time_str = "T" + datetime.datetime.now().strftime("%Y_%m_%d_%H")

    for platform in platforms:
        print("------", platform, "-------")
        df, feature = app.load_data(platform)
        rf = app.randomforest(df, feature).to_dict()
        output[platform] = rf
        rf = {platform : rf}
        app.insert_feature_importance(rf, time_str)
        
    
    # json으로 저장
    import json, os
    
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), f'importance_data/feature_importance_{time_str}.json') 
    print(out_dir)
    with open(out_dir, 'w') as f:
        json.dump(output, f, indent=4, ensure_ascii=False)

    app.close()
