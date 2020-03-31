import ast, json, time

from datetime import datetime
import pandas as pd
from py2neo import Graph
from urllib.parse import urlparse

from .DfHelper import DfHelper

class Neo4jDataAccess:
    BATCH_SIZE=2000
    
    def __init__(self, debug=False): 
        self.creds = {}
        self.debug=debug
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
                            tweet.job_id = t.job_id,
                            tweet.hashtags = t.hashtags,
                            tweet.hydrated = 'FULL',
                            tweet.type = t.tweet_type
                        ON MATCH SET 
                            tweet.text = t.text,
                            tweet.favorite_count = t.favorite_count,
                            tweet.retweet_count = t.retweet_count,
                            tweet.record_updated_at = timestamp(),
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
                            user.job_name = t.job_id
                        ON MATCH SET 
                            user.name = t.user_name,
                            user.screen_name = t.user_screen_name,
                            user.followers_count = t.user_followers_count,
                            user.friends_count = t.user_friends_count,
                            user.user_profile_image_url = t.user_profile_image_url,
                            user.location = t.user_location,
                            user.created_at = t.user_created_at,
                            user.record_updated_at = timestamp(),
                            user.job_name = t.job_id                                

                    //Add Reply to tweets if needed
                    FOREACH(ignoreMe IN CASE WHEN t.tweet_type='REPLY' THEN [1] ELSE [] END | 
                        MERGE (retweet:Tweet {id:t.reply_tweet_id})            
                            ON CREATE SET retweet.id=t.reply_tweet_id,
                            retweet.record_created_at = timestamp(),
                            retweet.job_id = t.job_id,
                            retweet.hydrated = 'PARTIAL'
                    )
                    
                    //Add QUOTE_RETWEET to tweets if needed
                    FOREACH(ignoreMe IN CASE WHEN t.tweet_type='QUOTE_RETWEET' THEN [1] ELSE [] END | 
                        MERGE (quoteTweet:Tweet {id:t.quoted_status_id})            
                            ON CREATE SET quoteTweet.id=t.quoted_status_id,
                            quoteTweet.record_created_at = timestamp(),
                            quoteTweet.job_id = t.job_id,
                            quoteTweet.hydrated = 'PARTIAL'
                    )
                    
                    //Add RETWEET to tweets if needed
                    FOREACH(ignoreMe IN CASE WHEN t.tweet_type='RETWEET' THEN [1] ELSE [] END | 
                        MERGE (retweet:Tweet {id:t.retweet_id})            
                            ON CREATE SET retweet.id=t.retweet_id,
                            retweet.record_created_at = timestamp(),
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
                            user.job_name = t.job_id
                    WITH user, tweet                                        
                    MERGE (tweet)-[:MENTIONED]->(user)                  
        """
        
        self.urls = """UNWIND $urls AS t                    
                    MATCH (tweet:Tweet {id:t.tweet_id})
                    MERGE (url:Url {full_url:t.url})                    
                        ON CREATE SET 
                            url.full_url = t.url,
                            url.job_name = t.job_id,
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

    
    def __get_neo4j_graph(self, role_type):
        with open('neo4jcreds.json') as json_file:
            creds = json.load(json_file)
            res = list(filter(lambda c: c["type"]==role_type, creds))
            if len(res): 
                creds = res[0]["creds"]
                self.graph = Graph(host=creds['host'], port=creds['port'], user=creds['user'], password=creds['password'])
            else: 
                self.graph = None
            return self.graph
    
    def save_parquet_df_to_graph(self, df, job_name):
        pdf = DfHelper(self.debug).normalize_parquet_dataframe(df)
        if self.debug: print('Saving to Neo4j')
        self.__save_df_to_graph(pdf, job_name)

    # Get the status of a DataFrame of Tweets by id.  Returns a dataframe with the hydrated status
    def get_tweet_hydrated_status_by_id(self, df, job_name='generic_job'):
        if 'id' in df:
            graph = self.__get_neo4j_graph('reader')  
            ids=[]
            for index, row in df.iterrows():
                ids.append({'id': int(row['id'])})
            res = graph.run(self.fetch_tweet_status, ids = ids).to_data_frame()
            res=res.rename(columns={'tweet.id': 'id', 'tweet.hydrated': 'hydrated'})
            return res
        else:
            raise Exception('Parameter df must be a DataFrame with a column named "id" ')
        
    # This saves the User and Tweet data right now
    def __save_df_to_graph(self, df, job_name):
        graph = self.__get_neo4j_graph('writer')  
        global_tic=time.perf_counter()      
        params = []
        mention_params = []
        url_params = []
        tic=time.perf_counter()
        for index, row in df.iterrows():
            #determine the type of tweet
            tweet_type='TWEET'
            if row["in_reply_to_status_id"] is not None and row["in_reply_to_status_id"] >0:
                tweet_type="REPLY"
            elif row["quoted_status_id"] is not None and row["quoted_status_id"] >0:
                tweet_type="QUOTE_RETWEET"                
            elif row["retweet_id"] is not None and row["retweet_id"] >0:
                tweet_type="RETWEET"
            params.append({'tweet_id': row['status_id'], 
                           'text': row['full_text'],    
                           'tweet_created_at': row['created_at'].to_pydatetime(),  
                           'favorite_count': row['favorite_count'],        
                           'retweet_count': row['retweet_count'],         
                           'tweet_type': tweet_type,                                              
                           'job_id': job_name,                                              
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
                           'retweet_id': row['retweet_id'],    
                          })
            
            #if there are urls then populate the url_params
            if row['urls']:
                url_params = self.__parse_urls(row, url_params, job_name)
            #if there are user_mentions then populate the mentions_params
            if row['user_mentions']:
                for m in row['user_mentions']:
                    mention_params.append({
                        'tweet_id': row['status_id'], 
                        'user_id': m['id'],                            
                        'user_name': m['name'],                                                      
                        'user_screen_name': m['screen_name'],                                           
                        'job_id': job_name,    
                    })
            if index % self.BATCH_SIZE == 0 and index>0:
                self.__write_to_neo(params, url_params, mention_params)
                toc=time.perf_counter()
                if self.debug: print(f"Neo4j Periodic Save Complete in  {toc - tic:0.4f} seconds")
                params = []
                mention_params = []
                url_params = []
                tic=time.perf_counter()
        
        self.__write_to_neo(params, url_params, mention_params)
        toc=time.perf_counter()
        print(f"Neo4j Import Complete in  {toc - global_tic:0.4f} seconds")
        
    def __write_to_neo(self, params, url_params, mention_params):
        try: 
            tx = self.graph.begin(autocommit=False)
            tx.run(self.tweetsandaccounts, tweets = params)
            tx.run(self.tweeted_rel, tweets = params)   
            tx.run(self.mentions, mentions = mention_params) 
            tx.run(self.urls, urls = url_params)                   
            tx.commit()
        except Exception as inst:
            print(type(inst))    # the exception instance
            print(inst.args)     # arguments stored in .args
            print(inst)          # __str__ allows args to be printed directly,
    
    def __normalize_hashtags(self, value):
        if value:
            hashtags=[]
            for h in value:
                hashtags.append(h['text'])
            return ','.join(hashtags)
        else:
            return None
    
    def __parse_urls(self, row, url_params, job_name):
        for u in row['urls']:
            try: 
                parsed = urlparse(u['expanded_url'])
                url_params.append({
                    'tweet_id': row['status_id'], 
                    'url': u['expanded_url'],     
                    'job_id': job_name,        
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
                print(type(inst))    # the exception instance
                print(inst.args)     # arguments stored in .args
                print(inst)          # __str__ allows args to be printed directly,
        return url_params
