import ast
import json
import time
import re

import enum

from datetime import datetime
import pandas as pd
from neo4j import GraphDatabase, basic_auth
from urllib.parse import urlparse
import logging
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
from .DfHelper import DfHelper

logger = logging.getLogger('Neo4jDataAccess')

def dict_to_property_str(properties:Optional[dict] = None) -> str:

    def property_type_checker(property_value):
        if isinstance(property_value,int) or isinstance(property_value,float):
            pass
        elif isinstance(property_value,str):
            property_value = '''"''' + property_value.replace('"',r"\"") + '''"'''
        elif not property_value:
            property_value = ""
        return property_value

    resp:str = ""
    if properties:
        resp = "{"
        for key in properties.keys():
            resp += """{key}:{value},""".format(key=key,value=property_type_checker(properties[key]))
        resp = resp[:-1] + "}"
    return resp

def cypher_template_filler(cypher_template:str,data:dict) -> str:
    return cypher_template.format(**data).replace("\n","")

class Neo4jDataAccess:
    class NodeLabel(enum.Enum):
        Tweet = 'Tweet'
        Url = 'Url'
        Account = 'Account'

    class RelationshipLabel(enum.Enum):
        TWEETED = 'TWEETED'
        MENTIONED = 'MENTIONED'
        QUOTED = 'QUOTED'
        REPLIED = 'REPLIED'
        RETWEETED = 'RETWEETED'
        INCLUDES = 'INCLUDES'

    def __init__(self, debug=False, neo4j_creds=None, batch_size=2000, timeout="60s"):
        self.creds = neo4j_creds
        self.debug = debug
        self.timeout = timeout
        self.batch_size = batch_size
        self.tweetsandaccounts = """
                  UNWIND $tweets AS t
                      //Add the Tweet
                    MERGE (tweet:Tweet {id:t.tweet_id})
                        ON CREATE SET
                            tweet.text = t.text,
                            tweet.created_at = t.tweet_created_at,
                            tweet.favorite_count = t.favorite_count,
                            tweet.retweet_count = t.retweet_count,
                            tweet.record_created_at = timestamp(),
                            tweet.job_name = t.job_name,
                            tweet.job_id = t.job_id,
                            tweet.hashtags = t.hashtags,
                            tweet.hydrated = 'FULL',
                            tweet.type = t.tweet_type
                        ON MATCH SET
                            tweet.text = t.text,
                            tweet.favorite_count = t.favorite_count,
                            tweet.retweet_count = t.retweet_count,
                            tweet.record_updated_at = timestamp(),
                            tweet.job_name = t.job_name,
                            tweet.job_id = t.job_id,
                            tweet.hashtags = t.hashtags,
                            tweet.hydrated = 'FULL',
                            tweet.type = t.tweet_type

                    //Add Account
                    MERGE (user:Account {id:t.user_id})
                        ON CREATE SET
                            user.id = t.user_id,
                            user.name = t.name,
                            user.screen_name = t.user_screen_name,
                            user.followers_count = t.user_followers_count,
                            user.friends_count = t.user_friends_count,
                            user.location = t.user_location,
                            user.user_profile_image_url = t.user_profile_image_url,
                            user.created_at = t.user_created_at,
                            user.record_created_at = timestamp(),
                            user.job_name = t.job_name,
                            user.job_id = t.job_id
                        ON MATCH SET
                            user.name = t.user_name,
                            user.screen_name = t.user_screen_name,
                            user.followers_count = t.user_followers_count,
                            user.friends_count = t.user_friends_count,
                            user.user_profile_image_url = t.user_profile_image_url,
                            user.location = t.user_location,
                            user.created_at = t.user_created_at,
                            user.record_updated_at = timestamp(),
                            user.job_name = t.job_name,
                            user.job_id = t.job_id

                    //Add Reply to tweets if needed
                    FOREACH(ignoreMe IN CASE WHEN t.tweet_type='REPLY' THEN [1] ELSE [] END |
                        MERGE (retweet:Tweet {id:t.reply_tweet_id})
                            ON CREATE SET retweet.id=t.reply_tweet_id,
                            retweet.record_created_at = timestamp(),
                            retweet.job_name = t.job_name,
                            retweet.job_id = t.job_id,
                            retweet.hydrated = 'PARTIAL'
                    )

                    //Add QUOTE_RETWEET to tweets if needed
                    FOREACH(ignoreMe IN CASE WHEN t.tweet_type='QUOTE_RETWEET' THEN [1] ELSE [] END |
                        MERGE (quoteTweet:Tweet {id:t.quoted_status_id})
                            ON CREATE SET quoteTweet.id=t.quoted_status_id,
                            quoteTweet.record_created_at = timestamp(),
                            quoteTweet.job_name = t.job_name,
                            quoteTweet.job_id = t.job_id,
                            quoteTweet.hydrated = 'PARTIAL'
                    )

                    //Add RETWEET to tweets if needed
                    FOREACH(ignoreMe IN CASE WHEN t.tweet_type='RETWEET' THEN [1] ELSE [] END |
                        MERGE (retweet:Tweet {id:t.retweet_id})
                            ON CREATE SET retweet.id=t.retweet_id,
                            retweet.record_created_at = timestamp(),
                            retweet.job_name = t.job_name,
                            retweet.job_id = t.job_id,
                            retweet.hydrated = 'PARTIAL'
                    )
        """

        self.tweeted_rel = """UNWIND $tweets AS t
                    MATCH (user:Account {id:t.user_id})
                    MATCH (tweet:Tweet {id:t.tweet_id})
                    OPTIONAL MATCH (replied:Tweet {id:t.reply_tweet_id})
                    OPTIONAL MATCH (quoteTweet:Tweet {id:t.quoted_status_id})
                    OPTIONAL MATCH (retweet:Tweet {id:t.retweet_id})
                    WITH user, tweet, replied, quoteTweet, retweet

                    MERGE (user)-[r:TWEETED]->(tweet)

                    FOREACH(ignoreMe IN CASE WHEN tweet.type='REPLY' AND replied.id>0 THEN [1] ELSE [] END |
                        MERGE (tweet)-[:REPLYED]->(replied)
                    )

                    FOREACH(ignoreMe IN CASE WHEN tweet.type='QUOTE_RETWEET' AND quoteTweet.id>0 THEN [1] ELSE [] END |
                        MERGE (tweet)-[:QUOTED]->(quoteTweet)
                    )

                    FOREACH(ignoreMe IN CASE WHEN tweet.type='RETWEET' AND retweet.id>0 THEN [1] ELSE [] END |
                        MERGE (tweet)-[:RETWEETED]->(retweet)
                    )

        """

        self.mentions = """UNWIND $mentions AS t
                    MATCH (tweet:Tweet {id:t.tweet_id})
                    MERGE (user:Account {id:t.user_id})
                        ON CREATE SET
                            user.id = t.user_id,
                            user.mentioned_name = t.name,
                            user.mentioned_screen_name = t.user_screen_name,
                            user.record_created_at = timestamp(),
                            user.job_name = t.job_name,
                            user.job_id = t.job_id
                    WITH user, tweet
                    MERGE (tweet)-[:MENTIONED]->(user)
        """

        self.urls = """UNWIND $urls AS t
                    MATCH (tweet:Tweet {id:t.tweet_id})
                    MERGE (url:Url {full_url:t.url})
                        ON CREATE SET
                            url.full_url = t.url,
                            url.job_name = t.job_name,
                            url.job_id = t.job_id,
                            url.record_created_at = timestamp(),
                            url.schema=t.scheme,
                            url.netloc=t.netloc,
                            url.path=t.path,
                            url.params=t.params,
                            url.query=t.query,
                            url.fragment=t.fragment,
                            url.username=t.username,
                            url.password=t.password,
                            url.hostname=t.hostname,
                            url.port=t.port
                    WITH url, tweet
                    MERGE (tweet)-[:INCLUDES]->(url)
        """

        self.fetch_tweet_status = """UNWIND $ids AS i
                    MATCH (tweet:Tweet {id:i.id})
                    RETURN tweet.id, tweet.hydrated
        """

        self.fetch_tweet = """UNWIND $ids AS i
                    MATCH (tweet:Tweet {id:i.id})
                    RETURN tweet
        """

    def __get_neo4j_graph(self, role_type):
        creds = None
        logging.debug('role_type: %s', role_type)
        if not (self.creds is None):
            creds = self.creds
        else:
            with open('neo4jcreds.json') as json_file:
                creds = json.load(json_file)
        res = list(filter(lambda c: c["type"] == role_type, creds))
        if len(res):
            logging.debug("creds %s", res)
            creds = res[0]["creds"]
            uri = f'bolt://{creds["host"]}:{creds["port"]}'
            self.graph = GraphDatabase.driver(
                uri, auth=basic_auth(creds['user'], creds['password']), encrypted=False)
        else:
            self.graph = None
        return self.graph

    def get_from_neo(self, cypher, limit=1000):
        graph = self.__get_neo4j_graph('reader')
        # If the limit isn't set in the traversal then add it
        if not re.search('LIMIT', cypher, re.IGNORECASE):
            cypher = cypher + " LIMIT " + str(limit)
        with graph.session() as session:
            result = session.run(cypher, timeout=self.timeout)
            df = pd.DataFrame([dict(record) for record in result])
        return df.head(limit)

    def get_tweet_by_id(self, df, cols=[]):
        if 'id' in df:
            graph = self.__get_neo4j_graph('reader')
            ids = []
            for index, row in df.iterrows():
                ids.append({'id': int(row['id'])})
            with graph.session() as session:
                result = session.run(
                    self.fetch_tweet, ids=ids, timeout=self.timeout)
                res = pd.DataFrame([dict(record) for record in result])
            logging.debug('Response info: %s rows, %s columns: %s' %
                          (len(res), len(res.columns), res.columns))
            pdf = pd.DataFrame()
            for r in res.iterrows():
                props = {}
                for k in r[1]['tweet'].keys():
                    if cols:
                        if k in cols:
                            props.update({k: r[1]['tweet'][k]})
                    else:
                        props.update({k: r[1]['tweet'][k]})
                pdf = pdf.append(props, ignore_index=True)
            return pdf
        else:
            raise TypeError(
                'Parameter df must be a DataFrame with a column named "id" ')

    def save_enrichment_df_to_graph(self, label: NodeLabel, df: pd.DataFrame, job_name: str, job_id=None):
        if not isinstance(label, self.NodeLabel):
            raise TypeError('The label parameter is not of type NodeType')

        if not isinstance(df, pd.DataFrame):
            raise TypeError(
                'The df parameter is not of type Pandas.DataFrame')

        idColName = 'full_url' if label == self.NodeLabel.Url else 'id'
        statement = 'UNWIND $rows AS t'
        statement += '   MERGE (n:' + label.value + \
            ' {' + idColName + ':t.' + idColName + '}) ' + \
            ' SET '

        for column in df:
            if not column == idColName:
                statement += f' n.{column} = t.{column} '

        graph = self.__get_neo4j_graph('writer')
        with graph.session() as session:
            result = session.run(
                statement, rows=df.to_dict(orient='records'), timeout=self.timeout)

    def save_parquet_df_to_graph(self, df, job_name, job_id=None):
        pdf = DfHelper().normalize_parquet_dataframe(df)
        logging.info('Saving to Neo4j')
        self.__save_df_to_graph(pdf, job_name)

    # Get the status of a DataFrame of Tweets by id.  Returns a dataframe with the hydrated status
    def get_tweet_hydrated_status_by_id(self, df):
        if 'id' in df:
            graph = self.__get_neo4j_graph('reader')
            ids = []
            for index, row in df.iterrows():
                ids.append({'id': int(row['id'])})
            with graph.session() as session:
                result = session.run(self.fetch_tweet_status, ids=ids)
                res = pd.DataFrame([dict(record) for record in result])
            logging.debug('Response info: %s rows, %s columns: %s' %
                          (len(res), len(res.columns), res.columns))
            if len(res) == 0:
                return df[['id']].assign(hydrated=None)
            else:
                res = res.rename(
                    columns={'tweet.id': 'id', 'tweet.hydrated': 'hydrated'})
                # ensures hydrated=None if Neo4j does not answer for id
                res = df[['id']].merge(res, how='left', on='id')
                return res
        else:
            logging.debug('df columns %s', df.columns)
            raise Exception(
                'Parameter df must be a DataFrame with a column named "id" ')

    # This saves the User and Tweet data right now
    def __save_df_to_graph(self, df, job_name, job_id=None):
        graph = self.__get_neo4j_graph('writer')
        global_tic = time.perf_counter()
        params = []
        mention_params = []
        url_params = []
        tic = time.perf_counter()
        logging.debug('df columns %s', df.columns)
        for index, row in df.iterrows():
            # determine the type of tweet
            tweet_type = 'TWEET'
            if row["in_reply_to_status_id"] is not None and row["in_reply_to_status_id"] > 0:
                tweet_type = "REPLY"
            elif "quoted_status_id" in row and row["quoted_status_id"] is not None and row["quoted_status_id"] > 0:
                tweet_type = "QUOTE_RETWEET"
            elif "retweet_id" in row and row["retweet_id"] is not None and row["retweet_id"] > 0:
                tweet_type = "RETWEET"
            try:
                params.append({'tweet_id': row['status_id'],
                               'text': row['full_text'],
                               'tweet_created_at': row['created_at'].to_pydatetime(),
                               'favorite_count': row['favorite_count'],
                               'retweet_count': row['retweet_count'],
                               'tweet_type': tweet_type,
                               'job_id': job_id,
                               'job_name': job_name,
                               'hashtags': self.__normalize_hashtags(row['hashtags']),
                               'user_id': row['user_id'],
                               'user_name': row['user_name'],
                               'user_location': row['user_location'],
                               'user_screen_name': row['user_screen_name'],
                               'user_followers_count': row['user_followers_count'],
                               'user_friends_count': row['user_friends_count'],
                               'user_created_at': pd.Timestamp(row['user_created_at'], unit='s').to_pydatetime(),
                               'user_profile_image_url': row['user_profile_image_url'],
                               'reply_tweet_id': row['in_reply_to_status_id'],
                               'quoted_status_id': row['quoted_status_id'],
                               'retweet_id': row['retweet_id'] if 'retweet_id' in row else None,
                               })
            except Exception as e:
                logging.error('params.append exn', e)
                logging.error('row', row)
                raise e

            # if there are urls then populate the url_params
            if row['urls']:
                url_params = self.__parse_urls(
                    row, url_params, job_name, job_id)
            # if there are user_mentions then populate the mentions_params
            if row['user_mentions']:
                for m in row['user_mentions']:
                    mention_params.append({
                        'tweet_id': row['status_id'],
                        'user_id': m['id'],
                        'user_name': m['name'],
                        'user_screen_name': m['screen_name'],
                        'job_id': job_id,
                        'job_name': job_name,
                    })
            if index % self.batch_size == 0 and index > 0:
                self.__write_to_neo(params, url_params, mention_params)
                toc = time.perf_counter()
                logging.info(
                    f'Neo4j Periodic Save Complete in  {toc - tic:0.4f} seconds')
                params = []
                mention_params = []
                url_params = []
                tic = time.perf_counter()

        self.__write_to_neo(params, url_params, mention_params)
        toc = time.perf_counter()
        logging.info(
            f"Neo4j Import Complete in  {toc - global_tic:0.4f} seconds")

    def __write_to_neo(self, params, url_params, mention_params):
        try:
            with self.graph.session() as session:
                session.run(self.tweetsandaccounts,
                            tweets=params, timeout=self.timeout)
                session.run(self.tweeted_rel, tweets=params,
                            timeout=self.timeout)
                session.run(self.mentions, mentions=mention_params,
                            timeout=self.timeout)
                session.run(self.urls, urls=url_params, timeout=self.timeout)
        except Exception as inst:
            logging.error('Neo4j Transaction error')
            logging.error(type(inst))    # the exception instance
            logging.error(inst.args)     # arguments stored in .args
            # __str__ allows args to be printed directly,
            logging.error(inst)
            raise inst

    def __normalize_hashtags(self, value):
        if value:
            hashtags = []
            for h in value:
                hashtags.append(h['text'])
            return ','.join(hashtags)
        else:
            return None

    def __parse_urls(self, row, url_params, job_name, job_id=None):
        for u in row['urls']:
            try:
                parsed = urlparse(u['expanded_url'])
                url_params.append({
                    'tweet_id': row['status_id'],
                    'url': u['expanded_url'],
                    'job_id': job_id,
                    'job_name': job_name,
                    'schema': parsed.scheme,
                    'netloc': parsed.netloc,
                    'path': parsed.path,
                    'params': parsed.params,
                    'query': parsed.query,
                    'fragment': parsed.fragment,
                    'username': parsed.username,
                    'password': parsed.password,
                    'hostname': parsed.hostname,
                    'port': parsed.port,
                })
            except Exception as inst:
                logging.error(type(inst))    # the exception instance
                logging.error(inst.args)     # arguments stored in .args
                # __str__ allows args to be printed directly,
                logging.error(inst)
        return url_params

    def reset_id_store(self):
        self.study_triald_and_neo4j_id_pairs = {}
        self.drug_or_synonym_name_and_neo4j_id_pairs = {}
        self.url_and_neo4j_id_pairs = {}
    
    def close(self):
        self._driver.close()
    
    @staticmethod
    def _merge_node(tx, node_type, properties:Optional[dict] = None):



        data:dict = {
            "node_type":node_type,
            "properties":dict_to_property_str(properties)
        }
        base_cypher = """
            MERGE (n:{node_type} {properties})
            RETURN id(n)
        """

        result = tx.run(cypher_template_filler(base_cypher,data))
        return result.single()[0]
    
    @staticmethod
    def _merge_edge(tx, from_id, to_id, edge_type, properties:Optional[dict] = None, direction = ">"):
        if not direction in [">",""]:
            raise ValueError

        data:dict = {
            "from_id":int(from_id),
            "to_id":int(to_id),
            "edge_type":edge_type,
            "direction":direction,
            "properties":dict_to_property_str(properties) 
        }
        base_cypher = """
            MATCH (from)
            WHERE ID(from) = {from_id}
            MATCH (to)
            WHERE ID(to) = {to_id}
            MERGE (from)-[r:{edge_type} {properties}]-{direction}(to)
            RETURN id(r)
        """
        result = tx.run(cypher_template_filler(base_cypher,data))
        return result.single()[0]

    def merge_studies(self,studies:pd.DataFrame):
        node_merging_func = self._merge_node
        with self._driver.session() as session:
            logger.info("> Merging Studies Job is Started")
            count_node = 0
            prev_count_node = 0
            
            for study in studies.to_dict('records'):
                node_type = "Study"
                properties:dict = study
                study_id = session.write_transaction(node_merging_func, node_type, properties)
                self.study_triald_and_neo4j_id_pairs[study["trial_id"]] = study_id
                count_node += 1
                if count_node > prev_count_node + 1000:
                    prev_count_node = count_node
                    logger.info("> {} nodes already merged".format(count_node)) 

        logger.info("> Merging Studies Job is >> Done << with {} nodes merged".format(count_node)) 

    def merge_drugs_synonyms_and_link_between(self,drug_vocab):
        node_merging_func = self._merge_node
        edge_merging_func = self._merge_edge
        with self._driver.session() as session:
            logger.info("> Merging Drugs and Synonyms Job is Started to merge {} drugs with synonyms".format(len(drug_vocab)))
            count_node = 0
            count_edge = 0
            prev_count_node = 0
            prev_count_edge = 0

            for drug in drug_vocab.keys():
                node_type = "Drug"
                properties:dict = {
                    "name":drug
                }
                
                drug_id = session.write_transaction(node_merging_func, node_type, properties)
                self.drug_or_synonym_name_and_neo4j_id_pairs[drug] = drug_id
                count_node += 1
                if isinstance(drug_vocab[drug],list):
                    for synonym in drug_vocab[drug]:
                        node_type = "Synonym"
                        properties:dict = {
                            "name":synonym
                        }
                        synonym_id = session.write_transaction(node_merging_func, node_type, properties)
                        self.drug_or_synonym_name_and_neo4j_id_pairs[synonym] = synonym_id
                        count_node += 1

                        edge_type = "KNOWN_AS"
                        session.write_transaction(edge_merging_func, drug_id, synonym_id, edge_type)
                        count_edge += 1
                
                if count_node > prev_count_node + 1000 or  count_edge > prev_count_edge + 1000:
                    prev_count_node = count_node
                    prev_count_edge = count_edge
                    logger.debug("> {} nodes and {} edges already merged".format(count_node,count_edge)) 
                    
            logger.info("> Merging Drugs and Synonyms Job is >> Done << with {} nodes and {} edges merged".format(count_node,count_edge))

    def merge_drug_to_study_rels(self,edges:list):
        edge_merging_func = self._merge_edge

        if self.study_triald_and_neo4j_id_pairs != {} and self.drug_or_synonym_name_and_neo4j_id_pairs != {}:
            study_id_lookup:dict = self.study_triald_and_neo4j_id_pairs
            drug_id_lookup:dict = self.drug_or_synonym_name_and_neo4j_id_pairs
            with self._driver.session() as session:
                logger.info("> Merging connections of Drugs&Synonyms to Studies Job is Started with {} edges to merge".format(len(edges)))
                edge_type = "APPEARED_IN"
                edge_ids:list = [session.write_transaction(edge_merging_func, drug_id_lookup[drug], study_id_lookup[trial_id], edge_type) for drug, trial_id in edges]
                logger.info("> Merging connections of Drugs&Synonyms to Studies Job is Finished with {} edges merged".format(len(edge_ids)))
        else:
            logger.warning("No Neo4j ID information is available for merging connections of Drugs&Synonyms to Studies")

    def merge_url(self,urls:list):
        node_merging_func = self._merge_node
        with self._driver.session() as session:
            logger.info("> Merging Urls Job is Started")
            count_node = 0
            prev_count_node = 0
            
            for url in urls:
                node_type = "Url"
                properties:dict = {"url":url}
                url_id = session.write_transaction(node_merging_func, node_type, properties)
                self.url_and_neo4j_id_pairs[url] = url_id
                count_node += 1

                if count_node > prev_count_node + 1000:
                    prev_count_node = count_node
                    logger.debug("> {} nodes already merged".format(count_node)) 

        logger.info("> Merging Url Job is >> Done << with {} nodes merged".format(count_node))

    def merge_url_to_study_rels(self,edges:list):