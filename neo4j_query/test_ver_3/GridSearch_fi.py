from itertools import permutations

from numpy.core.fromnumeric import var
from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable
from tqdm import tqdm

from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from statsmodels.stats.outliers_influence import variance_inflation_factor
from patsy import dmatrix
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import accuracy_score
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
        print(len(data))
        # forest = RandomForestClassifier(random_state=42, n_estimators=1000, max_depth=5)
        # forest_params = {'max_depth': range(6, 13),
        #                 'max_features': ['auto', 'sqrt', 'log2'], 
        #                 'n_estimators': [300, 500, 800, 1000]}
        # (n_estimators=100, *, criterion="gini", max_depth=None, min_samples_split=2, min_samples_leaf=1, min_weight_fraction_leaf=0, max_features="auto", max_leaf_nodes=None, min_impurity_decrease=0,
        # forest = GridSearchCV(RandomForestClassifier(random_state=12, n_jobs=-1), forest_params, cv=3, verbose=1)

        df = pd.DataFrame(data, columns=feature)
        df = df.dropna()
        X = scaler.fit_transform(df.iloc[:, :-1])
        vif = pd.DataFrame()
        vif["VIF Factor"] = [variance_inflation_factor(X, i) for i in range(X.shape[1])]
        vif['features'] = feature[:-1]
        vif = vif.sort_values("VIF Factor").reset_index(drop=True)
        print(vif)
        y = df.iloc[:, -1]
        X, X_test, y, y_test = train_test_split(X, y, stratify=y, random_state=1, test_size=0.3)
        # forest.fit(X, y)
        # print("Best params:", forest.best_params_) 
        # print("Best cross validaton score", forest.best_score_)

        # print("RF train accuracy: %0.3f" % forest.score(X, y))
        # print("RF test accuracy: %0.3f" % forest.score(X_test, y_test))
        # print("RF train accuracy: %0.3f" % accuracy_score(y, forest.predict(X)))
        # print("RF test accuracy: %0.3f" % accuracy_score(y_test, forest.predict(X_test)))
        start_time = time.time()
        # importances = forest.feature_importances_
        elapsed_time = time.time() - start_time

        print(f"Elapsed time to compute the importances: "
            f"{elapsed_time:.3f} seconds")
        
        # forest_importances = pd.Series(importances, index=feature[:-1])
        # print(forest_importances)
        # return forest_importances  
        return 0   

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
                        feature = ['n_comment','bad','pagerank','betweeness','closeness','clustering', 'degree', 'rank']
                        features = [row[0]['n_comment'], row[0]['bad'],
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
            "MATCH (c:Content)"
            "WITH c "
            "MATCH (c:Content)<-[:RANKS]-(r:Rank { platform: $platform }) "
            "RETURN c, avg(r.n) AS rank "
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
        rf = app.randomforest(df, feature)
        # rf = app.randomforest(df, feature).to_dict()
        # output[platform] = rf
        # rf = {platform : rf}
        # app.insert_feature_importance(rf, time_str)
        
    
    # # json으로 저장
    # import json, os
    
    # out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), f'importance_data/feature_importance_{time_str}.json') 
    # print(out_dir)
    # with open(out_dir, 'w') as f:
    #     json.dump(output, f, indent=4, ensure_ascii=False)

    app.close()
