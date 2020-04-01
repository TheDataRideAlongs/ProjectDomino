import ast
import pandas as pd
from datetime import datetime
import time

import logging
logger = logging.getLogger('DfHelper')

class DfHelper:    
    def __init__(self, debug=False): 
        self.debug=debug
    
    def __clean_timeline_tweets(self, pdf):
        #response_gdf = gdf[ (gdf['in_reply_to_status_id'] > 0) | (gdf['quoted_status_id'] > 0) ].drop_duplicates(['id'])
        pdf = pdf.rename(columns={'id': 'status_id', 'id_str': 'status_id_str'})
        #pdf = pdf.reset_index(drop=True)
        return pdf
    
    def normalize_parquet_dataframe(self, df):
        pdf = df\
            .pipe(self.__clean_timeline_tweets)\
            .pipe(self.__clean_datetimes)\
            .pipe(self.__clean_retweeted)\
            .pipe(self.__tag_status_type)\
            .pipe(self.__flatten_retweets)\
            .pipe(self.__flatten_quotes)\
            .pipe(self.__flatten_users)\
            .pipe(self.__flatten_entities)
        return pdf

    def __clean_datetimes(self, pdf):
        if self.debug: print('cleaning datetimes...')
        try:
            pdf = pdf.assign(created_at=pd.to_datetime(pdf['created_at']))
            pdf = pdf.assign(created_date=pdf['created_at'].apply(lambda dt: dt.timestamp()))
        except Exception as e:
            print('Error __clean_datetimes', e)
            print(pdf)
            raise e
        if self.debug: print('   ...cleaned')
        return pdf

    #some reason always False
    #this seems to match full_text[:2] == 'RT'
    def __clean_retweeted(self, pdf):
        return pdf.assign(retweeted=pdf['retweeted_status'] != 'None')

    def __update_to_type(self, row):
        if row['is_quote_status']:
            return 'retweet_quote'
        if row['retweeted']:
            return 'retweet'
        if row['in_reply_to_status_id'] > 0:
            return 'reply'
        return 'original'

    def __tag_status_type(self, pdf):
        ##only materialize required fields..
        if self.debug: print('tagging status...')
        pdf2 = pdf\
            .assign(status_type=pdf[['is_quote_status', 'retweeted', 'in_reply_to_status_id']].apply(self.__update_to_type, axis=1))
        if self.debug: print('   ...tagged')
        return pdf2

    def __flatten_status_col(self, pdf, col, status_type, prefix):
        debug_retweets_flattened = None
        debug_retweets = None
        try:
            logger.debug('flattening %s...', col)
            logger.debug('    %s x %s: %s', len(pdf), len(pdf.columns), pdf.columns)
            if len(pdf) == 0:
                logger.debug('Warning: did not add mt case col output addition - pdf')
                return pdf
            #retweet_status -> hash -> lookup json for hash -> pull out id/created_at/user_id
            pdf_hashed = pdf.assign(hashed=pdf[col].apply(hash))
            retweets = pdf_hashed[ pdf_hashed['status_type'] == status_type ][['hashed', col]]\
                .drop_duplicates('hashed').reset_index(drop=True)
            if len(retweets) == 0:
                logger.debug('Warning: did not add mt case col output addition - retweets')
                return pdf
            #print('sample', retweets[col].head(10), retweets[col].apply(type))
            retweets_flattened = pd.io.json.json_normalize(
                retweets[col].replace("(").replace(")")\
                    .apply(self.__try_load))
            if len(retweets_flattened.columns) == 0:
                logger.debug('No tweets of type %s, early exit', status_type)
                return pdf
            debug_retweets_flattened = retweets_flattened
            debug_retweets = retweets
            logger.debug('   ... fixing dates')
            logger.debug('avail cols of %s x %s: %s', len(retweets_flattened), len(retweets_flattened.columns), retweets_flattened.columns)
            logger.debug(retweets_flattened)
            if 'created_at' in retweets_flattened:
                retweets_flattened = retweets_flattened.assign(
                    created_at = pd.to_datetime(retweets_flattened['created_at']).apply(lambda dt: dt.timestamp))
            if 'user.id' in retweets_flattened:
                retweets_flattened = retweets_flattened.assign(                
                    user_id = retweets_flattened['user.id'])
            logger.debug('   ... fixing dates') 
            retweets = retweets[['hashed']]\
                .assign(**{
                    prefix + c: retweets_flattened[c] 
                    for c in retweets_flattened if c in ['id', 'created_at', 'user_id']
                })
            logger.debug('   ... remerging')
            pdf_with_flat_retweets = pdf_hashed.merge(retweets, on='hashed', how='left').drop(columns='hashed')
            logger.debug('   ...flattened: %s', pdf_with_flat_retweets.shape)    
            return pdf_with_flat_retweets
        except Exception as e:
            logger.error(('Exception __flatten_status_col', e))
            logger.error(('params', col, status_type, prefix))
            #print(pdf[:10])
            logger.error('cols debug_retweets %s x %s : %s',
                len(debug_retweets), len(debug_retweets.columns), debug_retweets.columns)
            logger.error('--------')
            logger.error(debug_retweets[:3])
            logger.error('--------')            
            logger.error('cols debug_retweets_flattened %s x %s : %s',
                len(debug_retweets_flattened), len(debug_retweets_flattened.columns), debug_retweets_flattened.columns)
            logger.error('--------')
            logger.error(debug_retweets_flattened[:3])
            logger.error('--------')
            logger.error(debug_retweets_flattened['created_at'])
            logger.error('--------')
            raise e

    def __flatten_retweets(self, pdf):
        if self.debug: print('flattening retweets...')
        pdf2 = self.__flatten_status_col(pdf, 'retweeted_status', 'retweet', 'retweet_')
        if self.debug: print('   ...flattened', pdf2.shape)    
        return pdf2

    def __flatten_quotes(self, pdf):
        if self.debug: print('flattening quotes...')
        pdf2 = self.__flatten_status_col(pdf, 'quoted_status', 'retweet_quote', 'quote_')
        if self.debug: print('   ...flattened', pdf2.shape)    
        return pdf2

    def __flatten_users(self, pdf):
        if self.debug: print('flattening users')
        pdf_user_cols = pd.io.json.json_normalize(pdf['user'].replace("(").replace(")").apply(ast.literal_eval))
        pdf2 = pdf.assign(**{
            'user_' + c: pdf_user_cols[c] 
            for c in pdf_user_cols if c in [
                'id', 'screen_name', 'created_at', 'followers_count', 'friends_count', 'favourites_count', 
                'utc_offset', 'time_zone', 'verified', 'statuses_count', 'profile_image_url', 'location',
                'name', 'description'
            ]})
        if self.debug: print('   ... fixing dates')
        pdf2 = pdf2.assign(user_created_at=pd.to_datetime(pdf2['user_created_at']).apply(lambda dt: dt.timestamp()))
        if self.debug: print('   ...flattened')
        return pdf2
    def __flatten_entities(self, pdf):
        if self.debug: print('flattening urls')
        pdf_entities = pd.io.json.json_normalize(pdf['entities'].replace("(").replace(")").apply(ast.literal_eval))
        pdf['urls']=pdf_entities['urls']
        pdf['hashtags']=pdf_entities['hashtags']
        pdf['user_mentions']=pdf_entities['user_mentions']
        return pdf
    def __try_load(self, s):
        try:
            out = ast.literal_eval(s)
            return {
                k if type(k) == str else str(k): out[k]
                for k in out.keys()
            }
        except:
            if s != 0.0:
                if self.debug: print('bad s',s)
            return {}