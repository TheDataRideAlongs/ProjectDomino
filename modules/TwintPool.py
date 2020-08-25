import pyarrow as pa
import ProjectDomino.modules.twint.twint as twint
from urlextract import URLExtract
from datetime import datetime, timedelta

import logging
logger = logging.getLogger()

class TwintPool:
    
    def __init__(self, fh_job=None, job_name='noname'):
        self.fh = fh_job
        self.config = twint.Config()
        self.config.Limit = 100
        self.config.Pandas = True
        self.config.Hide_output = True
        self.config.Verified = None
        self.config.Username = None
        #self.config.User_full = True


    def twint_loop(self, since, until, stride_sec=600, limit=None):
        def get_unix_time(time_str):
            return datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
        since = get_unix_time(since)
        until = get_unix_time(until)
        t = since
        tweets_returned = 0
        
        while t < until and (not tweets_returned or tweets_returned < limit):
            t0 = t
            t1 = t+timedelta(seconds=stride_sec)
            self.config.Since = str(t0)
            self.config.Until = str(t1)
            logger.debug('Search step: %s-%s', t0, t1)
            twint.run.Search(self.config)
            tweets_returned += len(twint.storage.panda.Tweets_df)
            if len(twint.storage.panda.Tweets_df) > 0:
                logger.debug('Search hit, len %s', len(twint.storage.panda.Tweets_df))
                yield (twint.storage.panda.Tweets_df, t0, t1)
            else:
                logger.debug('not hits on %s - %s, continuing', t0, t1)
            t = t1
        logger.debug('twint_loop done, hits: %s', tweets_returned)
                

        
    def _get_term(self, Search="IngSoc", Since="1984-04-20 13:00:00", Until="1984-04-20 13:30:00", stride_sec=600, **kwargs):
        self.config.Search = Search
        self.config.Retweets = True
        for k,v in kwargs.items():
            setattr(self.config, k, v)
        #self.config.Search = term
        logger.debug('Search seq: %s-%s of %s', Search, Since, Until)
        for df,t0,t1 in self.twint_loop(Since, Until, stride_sec, self.config.Limit):
            yield (df, t0, t1)
    
    def _get_timeline(self, username, limit):
        self.config.Retweets = True
        self.config.Search = "from:"+username
        self.config.Limit = limit
        twint.run.Search(self.config)
        tweets_df = twint.storage.panda.Tweets_df
        return tweets_df
        


    
    
    def _get_user_info(self, username):
        self.config.Username = username
        self.config.Limit = 1
        twint.run.Lookup(self.config)
        return twint.storage.panda.User_df
    
    
    def twint_df_to_neo4j_df(self, df):
        neo4j_df = df.rename(columns={
            'id': 'status_id',
            'tweet': 'full_text',
            'created_at': 'created_at', # needs to be datetime
            'nlikes': 'favorite_count',
            'nretweets': 'retweet_count',
            #'user_id_str': 'user_id',
            'username': 'user_name',
            'name': 'user_screen_name'
            })
        
        
        def row_to_tweet_type(row):
            if row['quote_url'] is None or row['quote_url'] == '':
                return "QUOTE_RETWEET"
            elif row['retweet']:
                return "RETWEET"
            elif row['id'] == row['conversation_id']:
                return "TWEET"
            elif row['id'] != row['conversation_id']:
                return "REPLY"
            else:
                raise('wat')
        
        #def row_to_quoted_status_id(row):
            #if row['quote_url'] and len(row['quote_url']) > 0:
                #return row['quote_url'].split('/')[-1]
            #else:
                #return None
            
        def row_tweet_to_urls(row):
            extractor = URLExtract()
            return list(extractor.gen_urls(row['tweet']))
            
        neo4j_df['user_location'] = None
        neo4j_df['tweet_type_twint'] = df.apply(row_to_tweet_type, axis=1)
        neo4j_df['hashtags'] = df['hashtags'].apply(lambda x: [{'text': ht} for ht in x])
        neo4j_df['user_followers_count'] = None
        neo4j_df['user_friends_count'] = None
        #neo4j_df['user_created_at'] = None
        neo4j_df['user_profile_image_url'] = None
        neo4j_df['reply_tweet_id'] = None
        neo4j_df['user_mentions'] = df['tweet'].str.findall('@[\w]+')
        #neo4j_df['retweet_id'] is suspiciously empty (always)
        neo4j_df['retweeted_status'] = None
        neo4j_df['conversation_id'] = df['conversation_id']
        neo4j_df['created_at'] = (neo4j_df['created_at'] / 1000).apply(lambda n: datetime.fromtimestamp(n))
        
        #neo4j_df['quoted_status_id'] = df.apply(row_to_quoted_status_id, axis=1)
        #neo4j_df['is_quote_status'] = neo4j_df['quoted_status_id'] != None
        neo4j_df['in_reply_to_status_id'] = False
        neo4j_df['urls'] = df.apply(row_tweet_to_urls, axis=1)
        neo4j_df['user_id']= df['user_id']
        return neo4j_df
        

    def to_arrow(self, tweets_df):
        pass
        
     
        