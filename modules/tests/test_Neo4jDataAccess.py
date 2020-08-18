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
                    "user": "writer",
                    "password": "neo4j123"
                }
            },
            {
                "type": "reader",
                "creds": {
                    "host": "localhost",
                    "port": "7687",
                    "user": "reader",
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

        # Now add some account data
        user_data = [{'account_id': 1, 'hydrated': 'FULL'},
                     {'account_id': 2, 'hydrated': 'FULL'},
                     {'account_id': 3},
                     {'account_id': 4, 'hydrated': 'PARTIAL'},
                     {'account_id': 5, 'hydrated': 'PARTIAL'},
                     ]

        traversal = '''UNWIND $users AS u
            MERGE (account:Account {id:u.account_id})
                ON CREATE SET
                    account.hydrated = u.hydrated
        '''
        try:
            with graph.session() as session:
                session.run(traversal, users=user_data)
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

    def test_get_account_hydrated_status_by_id(self):
        df = Neo4jDataAccess(
            neo4j_creds=self.creds).get_account_hydrated_status_by_id(self.ids)

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
        df = pd.DataFrame([{'id': 1}])
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

    def test_get_tweet_by_id_wrong_parameter(self):
        with pytest.raises(TypeError) as excinfo:
            df = Neo4jDataAccess(
                neo4j_creds=self.creds).get_tweet_by_id('test')
        assert "df" in str(excinfo.value)

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

    def test_get_from_neo_with_unlimited(self):
        df = Neo4jDataAccess(neo4j_creds=self.creds).get_from_neo(
            'MATCH (n:Tweet) WHERE n.id>1000 RETURN n.id', limit=None)
        assert len(df) > 1000

        df = Neo4jDataAccess(neo4j_creds=self.creds).get_from_neo(
            'MATCH (n:Tweet) WHERE n.hydrated=\'FULL\' RETURN n.id LIMIT 5', limit=None)
        assert len(df) == 5

    def test_save_enrichment_df_to_graph_wrong_parameter_types(self):
        with pytest.raises(TypeError) as excinfo:
            res = Neo4jDataAccess(neo4j_creds=self.creds).save_enrichment_df_to_graph(
                'test', pd.DataFrame(), 'test')
        assert "label parameter" in str(excinfo.value)
        with pytest.raises(TypeError) as excinfo:
            res = Neo4jDataAccess(neo4j_creds=self.creds).save_enrichment_df_to_graph(
                Neo4jDataAccess.NodeLabel.Tweet, [], 'test')
        assert "Pandas.DataFrame" in str(excinfo.value)

    def test_save_enrichment_df_to_graph(self):
        df = pd.DataFrame([{'id': 111, 'text': 'Tweet 123'},
                           {'id': 222, 'text': 'Tweet 234'}
                           ])

        res = Neo4jDataAccess(neo4j_creds=self.creds).save_enrichment_df_to_graph(
            Neo4jDataAccess.NodeLabel.Tweet, df, 'test')

        df = Neo4jDataAccess(neo4j_creds=self.creds).get_tweet_by_id(
            df['id'].to_frame())
        assert len(df) == 2
        assert df.at[0, 'text'] == 'Tweet 123'
        assert df.at[1, 'text'] == 'Tweet 234'

    def test_save_enrichment_df_to_graph_new_nodes(self):
        df = pd.DataFrame([{'id': 555, 'text': 'Tweet 123'},
                           {'id': 666, 'text': 'Tweet 234'}
                           ])
        Neo4jDataAccess(neo4j_creds=self.creds).save_enrichment_df_to_graph(
            Neo4jDataAccess.NodeLabel.Tweet, df, 'test')

        df = Neo4jDataAccess(neo4j_creds=self.creds).get_tweet_by_id(
            df['id'].to_frame())
        assert len(df) == 2
        assert df.at[0, 'text'] == 'Tweet 123'
        assert df.at[1, 'text'] == 'Tweet 234'

    def test_save_enrichment_df_to_graph_multiple_properties(self):
        df = pd.DataFrame([{'id': 777, 'text': 'Tweet 123', 'favorite_count': 2},
                           {'id': 888, 'text': 'Tweet 234', 'favorite_count': 3}
                           ])
        Neo4jDataAccess(neo4j_creds=self.creds).save_enrichment_df_to_graph(
            Neo4jDataAccess.NodeLabel.Tweet, df, 'test')

        df = Neo4jDataAccess(neo4j_creds=self.creds).get_tweet_by_id(
            df['id'].to_frame())
        assert len(df) == 2
        assert df.at[0, 'text'] == 'Tweet 123'
        assert df.at[0, 'favorite_count'] == 2
        assert df.at[1, 'text'] == 'Tweet 234'
        assert df.at[1, 'favorite_count'] == 3

    def test_save_enrichment_df_to_graph_property_does_not_exist(self):
        df = pd.DataFrame([{'id': 777, 'text': 'Tweet 123', 'new_prop': 2}])
        with pytest.raises(Exception) as excinfo:
            Neo4jDataAccess(neo4j_creds=self.creds).save_enrichment_df_to_graph(
                Neo4jDataAccess.NodeLabel.Tweet, df, 'test')

        assert "create_propertykey" in str(excinfo.value)

    def test_get_neo4j_graph(self):
        res = Neo4jDataAccess(
            neo4j_creds=self.creds).get_neo4j_graph(Neo4jDataAccess.RoleType.READER)
        assert res is not None

    def test_get_neo4j_graph_wrong_parameter(self):
        with pytest.raises(TypeError) as excinfo:
            res = Neo4jDataAccess(
                neo4j_creds=self.creds).get_neo4j_graph('test')
        assert "role_type parameter" in str(excinfo.value)


def setup_cleanup(self):
    print("I want to perfrom some cleanup action!")
