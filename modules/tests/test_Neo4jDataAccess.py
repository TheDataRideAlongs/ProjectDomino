from modules.Neo4jDataAccess import Neo4jDataAccess
from neo4j import GraphDatabase, basic_auth
import pandas as pd
import pytest
import os
import sys
from pathlib import Path


class TestNeo4jDataAccess:
    @classmethod
    def setup_class(cls):
        cls.creds = [
            {
                "type": "writer",
                "creds": {
                    "host": "localhost",
                    "port": "7687",
                    "user": "neo4j",
                    "password": "neo4j123"
                }
            },
            {
                "type": "reader",
                "creds": {
                    "host": "localhost",
                    "port": "7687",
                    "user": "neo4j",
                    "password": "neo4j123"
                }
            }
        ]
        data = [{'tweet_id': 1, 'text': 'Tweet 1', 'hydrated': 'FULL'},
                {'tweet_id': 2, 'text': 'Tweet 2', 'hydrated': 'FULL'},
                {'tweet_id': 3, 'text': 'Tweet 3'},
                {'tweet_id': 4, 'text': 'Tweet 4', 'hydrated': 'PARTIAL'},
                {'tweet_id': 5, 'text': 'Tweet 5', 'hydrated': 'PARTIAL'},
                ]

        traversal = '''UNWIND $tweets AS t
            MERGE (tweet:Tweet {id:t.tweet_id})
                ON CREATE SET 
                    tweet.text = t.text,
                    tweet.hydrated = t.hydrated
        '''
        res = list(filter(lambda c: c["type"] == 'writer', cls.creds))
        creds = res[0]["creds"]
        uri = f'bolt://{creds["host"]}:{creds["port"]}'
        graph = GraphDatabase.driver(
            uri, auth=basic_auth(creds['user'], creds['password']), encrypted=False)
        try:
            with graph.session() as session:
                session.run(traversal, tweets=data)
            cls.ids = pd.DataFrame({'id': [1, 2, 3, 4, 5]})
        except Exception as err:
            print(err)

    def test_get_tweet_hydrated_status_by_id(self):
        df = Neo4jDataAccess(
            neo4j_creds=self.creds).get_tweet_hydrated_status_by_id(self.ids)

        assert len(df) == 5
        assert df[df['id'] == 1]['hydrated'][0] == 'FULL'
        assert df[df['id'] == 2]['hydrated'][1] == 'FULL'
        assert df[df['id'] == 3]['hydrated'][2] == None
        assert df[df['id'] == 4]['hydrated'][3] == 'PARTIAL'
        assert df[df['id'] == 5]['hydrated'][4] == 'PARTIAL'

    def test_save_parquet_to_graph(self):
        filename = os.path.join(os.path.dirname(__file__),
                                'data/2020_03_22_02_b1.snappy2.parquet')
        tdf = pd.read_parquet(filename, engine='pyarrow')
        Neo4jDataAccess(
            neo4j_creds=self.creds).save_parquet_df_to_graph(tdf, 'dave')
        assert True

        # Test get_tweet_by_id
    def test_get_tweet_by_id(self):
        df = pd.DataFrame([{"id": 1}])
        df = Neo4jDataAccess(
            neo4j_creds=self.creds).get_tweet_by_id(df)
        assert len(df) == 1
        assert df.at[0, 'id'] == 1
        assert df.at[0, 'text'] == 'Tweet 1'
        assert df.at[0, 'hydrated'] == 'FULL'

    def test_get_tweet_by_id_with_cols(self):
        df = pd.DataFrame([{"id": 1}])
        df = Neo4jDataAccess(
            neo4j_creds=self.creds).get_tweet_by_id(df, cols=['id', 'text'])
        assert len(df) == 1
        assert len(df.columns) == 2
        assert df.at[0, 'id'] == 1
        assert df.at[0, 'text'] == 'Tweet 1'

    def test_get_from_neo(self):
        df = Neo4jDataAccess(neo4j_creds=self.creds).get_from_neo(
            'MATCH (n:Tweet) WHERE n.hydrated=\'FULL\' RETURN n.id, n.text LIMIT 5')
        assert len(df) == 5
        assert len(df.columns) == 2

    def test_get_from_neo_with_limit(self):
        df = Neo4jDataAccess(neo4j_creds=self.creds).get_from_neo(
            'MATCH (n:Tweet) WHERE n.hydrated=\'FULL\' RETURN n.id, n.text LIMIT 5', limit=1)
        assert len(df) == 1
        assert len(df.columns) == 2

    def test_get_from_neo_with_limit_only(self):
        df = Neo4jDataAccess(neo4j_creds=self.creds).get_from_neo(
            'MATCH (n:Tweet) WHERE n.hydrated=\'FULL\' RETURN n.id, n.text', limit=1)
        assert len(df) == 1
        assert len(df.columns) == 2


def setup_cleanup(self):
    print("I want to perfrom some cleanup action!")
