###

from asyncore import write
from collections import deque, defaultdict
import datetime, gc, os, string, sys, time, uuid
from typing import Any, Literal, Optional
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.fs
import pyarrow.parquet as pq
import s3fs
import simplejson as json  # nan serialization
from twarc import Twarc

from .Timer import Timer
from .TwarcPool import TwarcPool
from .Neo4jDataAccess import Neo4jDataAccess
from .StatusArrow import KNOWN_FIELDS
from .TwintPool import TwintPool

import logging

logger = logging.getLogger('fh')

try:
    import cudf
except:
    logger.debug('Warning: no cudf')

#############################

###################

object_dtype = pd.Series([[1, 2], ['3', '4']]).dtype
string_dtype = 'object'
id_type = np.int64

### If col missing in df, create with below dtype & default val
EXPECTED_COLS = [
    ('contributors', 'object', []),
    ('coordinates', 'object', None),
    ('created_at', 'object', None),  # FIXME datetime[s]
    ('display_text_range', 'object', None),
    ('extended_entities', 'object', None),
    ('entities', 'object', None),
    ('favorited', np.bool, None),
    ('followers', 'object', None),
    ('favorite_count', np.int64, 0),
    ('full_text', 'object', None),
    ('geo', 'object', None),
    ('id', np.int64, None),
    ('id_str', string_dtype, None),
    ('in_reply_to_screen_name', string_dtype, None),
    ('in_reply_to_status_id', np.int64, None),
    ('in_reply_to_status_id_str', string_dtype, None),
    ('in_reply_to_user_id', np.int64, None),
    ('in_reply_to_user_id_str', string_dtype, None),
    ('is_quote_status', np.bool, None),
    ('lang', string_dtype, None),
    ('place', string_dtype, None),
    ('possibly_sensitive', np.bool_, False),
    ('quoted_status', string_dtype, 0.0),
    ('quoted_status_id', id_type, 0),
    ('quoted_status_id_str', string_dtype, None),
    ('quoted_status_permalink', string_dtype, None),
    ('in_reply_to_status_id', id_type, 0),
    ('in_reply_to_user_id', id_type, 0),
    ('retweet_count', np.int64, 0),
    ('retweeted', np.bool, None),
    ('retweeted_status', string_dtype, None),
    ('scopes', 'object', None),
    ('source', string_dtype, None),
    ('truncated', np.bool, None),
    ('user', string_dtype, None),
    ('withheld_in_countries', object_dtype, [])
]

DROP_COLS = ['withheld_in_countries']


#### PARQUET WRITER BARFS
# sizes_t = pa.struct({
#                'h': pa.int64(),
#                'resize': pa.string(),
#                'w': pa.int64()
#            })
#
# extended_entities_t = pa.struct({
#    'media': pa.list_(pa.struct({
#        'display_url': pa.string(),
#        'expanded_url': pa.string(),
#        'ext_alt_text': pa.string(),
#        'id': pa.int64(),
#        'id_str': pa.string(),
#        'indices': pa.list_(pa.int64()),
#        'media_url': pa.string(),
#        'media_url_https': pa.string(),
#        'sizes': pa.struct({
#            'large': sizes_t,
#            'medium': sizes_t,
#            'small': sizes_t,
#            'thumb': sizes_t
#        }),
#        'source_status_id': pa.int64(),
#        'source_status_id_str': pa.string(),
#        'source_user_id': pa.int64(),
#        'source_user_id_str': pa.string(),
#        'type': pa.string(),
#        'url': pa.string()
#    }))})


#############################



def make_serializable(df):

    try:
        pa.Table.from_pandas(df)
        return df
    except:
        logger.debug('Warning: df not serializable, attempt to clean')

    try:
        df_cleaned = df.infer_objects()
        pa.Table.from_pandas(df_cleaned)
        return df_cleaned
    except:
        logger.debug('Warning: df not serializable via infer_objects(), attempt per-column clean')

    for col in [ 'quote_url', 'place']:
        if col in df_cleaned.columns:
            df_cleaned[col] = df_cleaned[col].astype(str)

    bad_cols = []
    fatal_cols = []
    for col in df_cleaned.columns:
        try:
            pa.Table.from_pandas(df_cleaned[[col]])
        except Exception as e:
            try:
                as_str = df_cleaned[col].astype(str)
                df2 = df_cleaned[[col]]
                df2[col] = as_str
                pa.Table.from_pandas(df2)
                #good, save
                df_cleaned[col] = as_str
                bad_cols.append(col)
            except:
                fatal_cols.append(col)
    logger.debug('bad_cols: %s, fatal_cols (dropping): %s' , bad_cols, fatal_cols)
    return df_cleaned.drop(fatal_cols, axis=1)


class FirehoseJob:
    ###################

    MACHINE_IDS = (375, 382, 361, 372, 364, 381, 376, 365, 363, 362, 350, 325, 335, 333, 342, 326, 327, 336, 347, 332)
    SNOWFLAKE_EPOCH = 1288834974657

    EXPECTED_COLS = EXPECTED_COLS
    KNOWN_FIELDS = KNOWN_FIELDS
    DROP_COLS = DROP_COLS

    def __init__(self, creds=[], neo4j_creds=None, TWEETS_PER_PROCESS=100, TWEETS_PER_ROWGROUP=5000, save_to_neo=False,
                 PARQUET_SAMPLE_RATE_TIME_S=None, debug=False, BATCH_LEN=100, writers={'snappy': None},
                 tp = None,
                 write_to_disk: Optional[Literal['csv', 'json', 'parquet', 'parquet_s3']] = None,
                 write_opts: Optional[Any] = None
    ):
        self.queue = deque()
        self.writers = writers
        self.last_write_epoch = ''
        self.tp = tp
        self.current_table = None
        self.schema = pa.schema([
            (name, t)
            for (i, name, t) in KNOWN_FIELDS
        ])
        self.timer = Timer()
        self.debug = debug
        self.write_to_disk = write_to_disk
        self.write_opts = write_opts

        self.twarc_pool = TwarcPool([
            Twarc(o['consumer_key'], o['consumer_secret'], o['access_token'], o['access_token_secret'])
            for o in creds
        ])
        self.save_to_neo = save_to_neo
        self.TWEETS_PER_PROCESS = TWEETS_PER_PROCESS  # 100
        self.TWEETS_PER_ROWGROUP = TWEETS_PER_ROWGROUP  # 100 1KB x 1000 = 1MB uncompressed parquet
        self.PARQUET_SAMPLE_RATE_TIME_S = PARQUET_SAMPLE_RATE_TIME_S
        self.last_df = None
        self.last_arr = None
        self.last_write_arr = None
        self.last_writes_arr = []

        self.neo4j_creds = neo4j_creds

        self.BATCH_LEN = BATCH_LEN

        self.needs_to_flush = False

        self.__file_names = []

    def __del__(self):

        logger.debug('__del__')
        self.destroy()

    def destroy(self, job_name='generic_job'):
        logger.debug('flush before destroying..')
        self.flush(job_name)

        logger.debug('destroy %s' % self.writers.keys())

        for k in self.writers.keys():
            if not (self.writers[k] is None):
                logger.debug('Closing parquet writer %s' % k)
                writer = self.writers[k]
                writer.close()
                logger.debug('... sleep 1s...')
                time.sleep(1)
                self.writers[k] = None
                logger.debug('... sleep 1s...')
                time.sleep(1)
                logger.debug('... Safely closed %s' % k)
            else:

                logger.debug('Nothing to close for writer %s' % k)

    ###################

    def get_creation_time(self, id):
        return ((id >> 22) + 1288834974657)

    def machine_id(self, id):
        return (id >> 12) & 0b1111111111

    def sequence_id(self, id):
        return id & 0b111111111111

    ###################

    valid_file_name_chars = frozenset("-_%s%s" % (string.ascii_letters, string.digits))

    def clean_file_name(self, filename):
        return ''.join(c for c in filename if c in FirehoseJob.valid_file_name_chars)

    # clean series before reaches arrow
    def clean_series(self, series):
        try:
            identity = lambda x: x

            series_to_json_string = (lambda series: series.apply(lambda x: json.dumps(x, ignore_nan=True)))

            ##objects: put here to skip str coercion
            coercions = {
                'display_text_range': series_to_json_string,
                'contributors': identity,
                'created_at': lambda series: series.values.astype('unicode'),
                'possibly_sensitive': (lambda series: series.fillna(False)),
                'quoted_status_id': (lambda series: series.fillna(0).astype('int64')),
                'extended_entities': series_to_json_string,
                'in_reply_to_status_id': (lambda series: series.fillna(0).astype('int64')),
                'in_reply_to_user_id': (lambda series: series.fillna(0).astype('int64')),
                'scopes': series_to_json_string,
                'followers': series_to_json_string,
                'withheld_in_countries': series_to_json_string
            }
            if series.name in coercions.keys():
                return coercions[series.name](series)
            elif series.dtype.name == 'object':
                return series.values.astype('unicode')
            else:
                return series
        except Exception as exn:
            logger.error('coerce exn on col', series.name, series.dtype)
            logger.error('first', series[:1])
            logger.error(exn)
            return series

    # clean df before reaches arrow
    def clean_df(self, raw_df):
        self.timer.tic('clean', 1000)
        try:
            new_cols = {
                c: pd.Series([c_default] * len(raw_df), dtype=c_dtype)
                for (c, c_dtype, c_default) in FirehoseJob.EXPECTED_COLS
                if not c in raw_df
            }
            all_cols_df = raw_df.assign(**new_cols)
            sorted_df = all_cols_df.reindex(sorted(all_cols_df.columns), axis=1)
            return pd.DataFrame({c: self.clean_series(sorted_df[c]) for c in sorted_df.columns})
        except Exception as exn:
            logger.error('failed clean')
            logger.error(exn)
            raise exn
        finally:
            self.timer.toc('clean')

    def folder_last(self):
        return self.__folder_last

    def files(self):
        return self.__file_names.copy()

    # TODO <topic>/<year>/<mo>/<day>/<24hour_utc>_<nth>.parquet (don't clobber..)
    def pq_writer(self, table, job_name='generic_job'):
        try:
            self.timer.tic('write', 1000)

            job_name = self.clean_file_name(job_name)

            folder = "firehose_data/%s" % job_name
            logger.debug('make folder if not exists: %s' % folder)
            os.makedirs(folder, exist_ok=True)
            self.__folder_last = folder

            vanilla_file_suffix = 'vanilla2.parquet'
            snappy_file_suffix = 'snappy2.parquet'
            time_prefix = datetime.datetime.now().strftime("%Y_%m_%d_%H")
            run = 0
            file_prefix = ""
            while (file_prefix == "") \
                    or os.path.exists(file_prefix + vanilla_file_suffix) \
                    or os.path.exists(file_prefix + snappy_file_suffix):
                run = run + 1
                file_prefix = "%s/%s_b%s." % (folder, time_prefix, run)
            if run > 1:
                logger.debug('Starting new batch for existing hour')
            vanilla_file_name = file_prefix + vanilla_file_suffix
            snappy_file_name = file_prefix + snappy_file_suffix

            #########################################################

            #########################################################
            if ('vanilla' in self.writers) and (
                    (self.writers['vanilla'] is None) or self.last_write_epoch != file_prefix):
                logger.debug('Creating vanilla writer: %s', vanilla_file_name)
                try:
                    # first write
                    # os.remove(vanilla_file_name)
                    1
                except Exception as exn:
                    logger.debug(('Could not rm vanilla parquet', exn))
                self.writers['vanilla'] = pq.ParquetWriter(
                    vanilla_file_name,
                    schema=table.schema,
                    compression='NONE')
                self.__file_names.append(vanilla_file_name)

            if ('snappy' in self.writers) and (
                    (self.writers['snappy'] is None) or self.last_write_epoch != file_prefix):
                logger.debug('Creating snappy writer: %s', snappy_file_name)
                try:
                    # os.remove(snappy_file_name)
                    1
                except Exception as exn:
                    logger.error(('Could not rm snappy parquet', exn))
                self.writers['snappy'] = pq.ParquetWriter(
                    snappy_file_name,
                    schema=table.schema,
                    compression={
                        field.name.encode(): 'SNAPPY'
                        for field in table.schema
                    })
                self.__file_names.append(snappy_file_name)

            self.last_write_epoch = file_prefix
            ######################################################

            for name in self.writers.keys():
                try:
                    logger.debug('Writing %s (%s x %s)' % (
                        name, table.num_rows, table.num_columns))
                    self.timer.tic('writing_%s' % name, 20, 1)
                    writer = self.writers[name]
                    writer.write_table(table)
                    logger.debug('========')
                    logger.debug(table.schema)
                    logger.debug('--------')
                    logger.debug(table.to_pandas()[:10])
                    logger.debug('--------')
                    self.timer.toc('writing_%s' % name, table.num_rows)
                    #########
                    logger.debug('######## TRANSACTING')
                    self.last_write_arr = table
                    self.last_writes_arr.append(table)
                    #########
                except Exception as exn:
                    logger.error('... failed to write to parquet')
                    logger.error(exn)
                    raise exn
            logger.debug('######### ALL WRITTEN #######')

        finally:
            self.timer.toc('write')

    def flush(self, job_name="generic_job"):
        try:
            if not hasattr(self, 'current_table') or self.current_table is None or self.current_table.num_rows == 0:
                return
            logger.debug('writing to parquet then clearing current_table..')
            deferred_pq_exn = None
            try:
                self.pq_writer(self.current_table, job_name)
            except Exception as e:
                deferred_pq_exn = e
            try:
                if self.save_to_neo:
                    logger.debug('Writing to Neo4j')
                    Neo4jDataAccess(self.debug, self.neo4j_creds).save_parquet_df_to_graph(
                        self.current_table.to_pandas(), job_name)
                else:
                    logger.debug('Skipping Neo4j write')
            except Exception as e:
                logger.error('Neo4j write exn', e)
                raise e
            if not (deferred_pq_exn is None):
                raise deferred_pq_exn
        finally:
            logger.debug('flush clearing self.current_table')
            self.current_table = None

    def tweets_to_df(self, tweets):
        try:
            self.timer.tic('to_pandas', 1000)
            df = pd.DataFrame(tweets)
            df = df.drop(columns=FirehoseJob.DROP_COLS, errors='ignore')
            self.last_df = df
            return df
        except Exception as exn:
            logger.error('Failed tweets->pandas')
            logger.error(exn)
            raise exn
        finally:
            self.timer.toc('to_pandas')

    def df_with_schema_to_arrow(self, df, schema):
        try:
            self.timer.tic('df_with_schema_to_arrow', 1000)
            table = None
            try:
                # if len(df['followers'].dropna()) > 0:
                #    print('followers!')
                #    print(df['followers'].dropna())
                #    raise Exception('ok')
                table = pa.Table.from_pandas(df, schema)
                if len(df.columns) != len(schema):
                    logger.debug('=========================')
                    logger.debug('DATA LOSS WARNING: df has cols not in schema, dropping')  # reverse is an exn
                    for col_name in df.columns:
                        hits = [field for field in schema if field.name == col_name]
                        if len(hits) == 0:
                            logger.debug('-------')
                            logger.debug('arrow schema missing col %s ' % col_name)
                            logger.debug('df dtype', df[col_name].dtype)
                            logger.debug(df[col_name].dropna())
                            logger.debug('-------')
            except Exception as exn:
                logger.error('============================')
                logger.error('failed nth arrow from_pandas')
                logger.error('-------')
                logger.error(exn)
                logger.error('-------')
                try:
                    logger.error(('followers', df['followers'].dropna()))
                    logger.error('--------')
                    logger.error(('coordinates', df['coordinates'].dropna()))
                    logger.error('--------')
                    logger.error('dtypes: %s', df.dtypes)
                    logger.error('--------')
                    logger.error(df.sample(min(5, len(df))))
                    logger.error('--------')
                    logger.error('arrow')
                    logger.error([schema[k] for k in range(0, len(schema))])
                    logger.error('~~~~~~~~')
                    if not (self.current_table is None):
                        try:
                            logger.error(self.current_table.to_pandas()[:3])
                            logger.error('----')
                            logger.error(
                                [self.current_table.schema[k] for k in range(0, self.current_table.num_columns)])
                        except Exception as exn2:
                            logger.error(('cannot to_pandas print..', exn2))
                except:
                    1
                logger.error('-------')
                err_file_name = 'fail_' + str(uuid.uuid1())
                logger.error('Log failed batch and try to continue! %s' % err_file_name)
                df.to_csv('./' + err_file_name)
                raise exn
            for i in range(len(schema)):
                if not (schema[i].equals(table.schema[i])):
                    logger.error('EXN: Schema mismatch on col # %s', i)
                    logger.error(schema[i])
                    logger.error('-----')
                    logger.error(table.schema[i])
                    logger.error('-----')
                    raise Exception('mismatch on col # ' % i)
            return table
        finally:
            self.timer.toc('df_with_schema_to_arrow')

    def concat_tables(self, table_old, table_new):
        try:
            self.timer.tic('concat_tables', 1000)
            return pa.concat_tables([table_old, table_new])  # promote..
        except Exception as exn:
            logger.error('=========================')
            logger.error('Error combining arrow tables, likely new table mismatches old')
            logger.error('------- cmp')
            for i in range(0, table_old.num_columns):
                if i >= table_new.num_columns:
                    logger.error('new table does not have enough columns to handle %i' % i)
                elif table_old.schema[i].name != table_new.schema[i].name:
                    logger.error(('ith col name mismatch', i,
                                  'old', (table_old.schema[i]), 'vs new', table_new.schema[i]))
            logger.error('------- exn')
            logger.error(exn)
            logger.error('-------')
            raise exn
        finally:
            self.timer.toc('concat_tables')

    def process_tweets_notify_hydrating(self):
        if not (self.current_table is None):
            self.timer.toc('tweet', self.current_table.num_rows)
        self.timer.tic('tweet', 40, 40)

        self.timer.tic('hydrate', 40, 40)

    # Call process_tweets_notify_hydrating() before
    def process_tweets(self, tweets, job_name='generic_job'):

        self.timer.toc('hydrate')

        self.timer.tic('overall_compute', 40, 40)

        raw_df = self.tweets_to_df(tweets)
        df = self.clean_df(raw_df)

        table = None
        try:
            table = self.df_with_schema_to_arrow(df, self.schema)
        except Exception as e:
            # logger.error('conversion failed, skipping batch...')
            self.timer.toc('overall_compute')
            raise e

        self.last_arr = table

        if self.current_table is None:
            self.current_table = table
        else:
            self.current_table = self.concat_tables(self.current_table, table)

        out = self.current_table  # or just table (without intermediate concats since last flush?)

        if not (self.current_table is None) \
                and ((self.current_table.num_rows > self.TWEETS_PER_ROWGROUP) or self.needs_to_flush) \
                and self.current_table.num_rows > 0:
            self.flush(job_name)
            self.needs_to_flush = False
        else:
            1
            # print('skipping, has table ? %s, num rows %s' % (
            #    not (self.current_table is None),
            #    0 if self.current_table is None else self.current_table.num_rows))

        self.timer.toc('overall_compute')

        return out

    def process_tweets_generator(self, tweets_generator, job_name='generic_job'):

        def flusher(tweets_batch):
            try:
                self.needs_to_flush = True
                return self.process_tweets(tweets_batch, job_name)
            except Exception as e:
                # logger.debug('failed processing batch, continuing...')
                raise e

        tweets_batch = []
        last_flush_time_s = time.time()

        try:
            for tweet in tweets_generator:
                tweets_batch.append(tweet)

                if len(tweets_batch) > self.TWEETS_PER_PROCESS:
                    self.needs_to_flush = True
                elif not (self.PARQUET_SAMPLE_RATE_TIME_S is None) \
                        and time.time() - last_flush_time_s >= self.PARQUET_SAMPLE_RATE_TIME_S:
                    self.needs_to_flush = True
                    last_flush_time_s = time.time()

                if self.needs_to_flush:
                    try:
                        yield flusher(tweets_batch)
                    except Exception as e:
                        # logger.debug('Write fail, continuing..')
                        raise e
                    finally:
                        tweets_batch = []
            logger.debug('===== PROCESSED ALL GENERATOR TASKS, FINISHING ====')
            yield flusher(tweets_batch)
            logger.debug('/// FLUSHED, DONE')
        except KeyboardInterrupt as e:
            logger.debug('========== FLUSH IF SLEEP INTERRUPTED')
            self.destroy()
            gc.collect()
            logger.debug('explicit GC...')
            logger.debug('Safely exited!')

    ################################################################################

    def process_ids(self, ids_to_process, job_name=None):

        self.process_tweets_notify_hydrating()

        if job_name is None:
            job_name = "process_ids_%s" % (ids_to_process[0] if len(ids_to_process) > 0 else "none")

        for i in range(0, len(ids_to_process), self.BATCH_LEN):
            ids_to_process_batch = ids_to_process[i: (i + self.BATCH_LEN)]

            logger.info('Starting batch offset %s ( + %s) of %s', i, self.BATCH_LEN, len(ids_to_process))

            hydration_statuses_df = Neo4jDataAccess(self.debug, self.neo4j_creds) \
                .get_tweet_hydrated_status_by_id(pd.DataFrame({'id': ids_to_process_batch}))
            missing_ids = hydration_statuses_df[hydration_statuses_df['hydrated'] != 'FULL']['id'].tolist()

            logger.debug('Skipping cached %s, fetching %s, of requested %s' % (
                len(ids_to_process_batch) - len(missing_ids),
                len(missing_ids),
                len(ids_to_process_batch)))

            tweets = (tweet for tweet in self.twarc_pool.next_twarc().hydrate(missing_ids))

            for arr in self.process_tweets_generator(tweets, job_name):
                yield arr

    def process_id_file(self, path, job_name=None):

        pdf = None
        lst = None
        try:
            pdf = cudf.read_csv(path, header=None).to_pandas()
            lst = pdf['0'].to_list()
        except:
            pdf = pd.read_csv(path, header=None)
            lst = pdf[0].to_list()

        if job_name is None:
            job_name = "id_file_%s" % path
        logger.debug('loaded %s ids, hydrating..' % len(lst))

        for arr in self.process_ids(lst, job_name):
            yield arr

    def search(self, input="", job_name=None):

        self.process_tweets_notify_hydrating()

        if job_name is None:
            job_name = "search_%s" % input[:20]

        tweets = (tweet for tweet in self.twarc_pool.next_twarc().search(input))

        self.process_tweets_generator(tweets, job_name)

    def search_stream_by_keyword(self, input="", job_name=None):

        self.process_tweets_notify_hydrating()

        if job_name is None:
            job_name = "search_stream_by_keyword_%s" % input[:20]

        tweets = [tweet for tweet in self.twarc_pool.next_twarc().filter(track=input)]

        self.process_tweets(tweets, job_name)

    def search_by_location(self, input="", job_name=None):

        self.process_tweets_notify_hydrating()

        if job_name is None:
            job_name = "search_by_location_%s" % input[:20]

        tweets = [tweet for tweet in self.twarc_pool.next_twarc().filter(locations=input)]

        self.process_tweets(tweets, job_name)

    #
    def user_timeline(self, input=[""], job_name=None, **kwargs):
        if not (type(input) == list):
            input = [input]
        try:
            self.process_tweets_notify_hydrating()

            if job_name is None:
                job_name = "user_timeline_%s_%s" % (len(input), '_'.join(input))

            for user in input:
                logger.debug('starting user %s' % user)
                tweet_count = 0
                for tweet in self.twarc_pool.next_twarc().timeline(screen_name=user, **kwargs):
                    # logger.debug('got user', user, 'tweet', str(tweet)[:50])
                    self.process_tweets([tweet], job_name)
                    tweet_count = tweet_count + 1
                logger.debug('    ... %s tweets' % tweet_count)

            self.destroy()
        except KeyboardInterrupt as e:
            logger.debug('Flushing..')
            self.destroy(job_name)
            logger.debug('Explicit GC')
            gc.collect()
            logger.debug('Safely exited!')

    def ingest_range(self, begin, end, job_name=None):  # This method is where the magic happens

        if job_name is None:
            job_name = "ingest_range_%s_to_%s" % (begin, end)

        for epoch in range(begin, end):  # Move through each millisecond
            time_component = (epoch - FirehoseJob.SNOWFLAKE_EPOCH) << 22
            for machine_id in FirehoseJob.MACHINE_IDS:  # Iterate over machine ids
                for sequence_id in [0]:  # Add more sequence ids as needed
                    twitter_id = time_component + (machine_id << 12) + sequence_id
                    self.queue.append(twitter_id)
                    if len(self.queue) >= self.TWEETS_PER_PROCESS:
                        ids_to_process = []
                        for i in range(0, self.TWEETS_PER_PROCESS):
                            ids_to_process.append(self.queue.popleft())
                        self.process_ids(ids_to_process, job_name)

    ###############################

    def _maybe_write_batch(
        self,
        df,
        write_to_disk: Optional[Literal['csv', 'json', 'parquet']] = None,
        id: Optional[str] = None,
        write_opts = {}
    ):
        write_to_disk = write_to_disk or self.write_to_disk

        logger.info('_maybe_write_batch: write_to_disk=%s, id=%s', write_to_disk, id)

        if write_to_disk is None:
            return
        if id is None:
            raise ValueError('need id to write to disk')
        
        print(f'writing batch {id} to disk: shape {df.shape}')

        if write_to_disk == 'csv':
            df.to_csv(f'/output/{id}.csv')
        elif write_to_disk == 'json':
            df.to_json(f'/output/{id}.json')
        elif write_to_disk == 'parquet':
            df_cleaned = make_serializable(df)
            df_cleaned.to_parquet(f'/output/{id}.parquet', compression='snappy')
        elif write_to_disk == 'parquet_s3':

            #s3_filepath = 'dt-phase1/data.parquet'
            s3_filepath = write_opts['s3_filepath']
            s3fs_options = write_opts['s3fs_options'] #key=S3_ACCESS_KEY, secret=S3_SECRET_KEY
            compression = (
                write_opts['compression']
                if 'compression' in write_opts and len(write_opts['compression']) > 0 else
                'snappy'
            )

            s3fs_instance = s3fs.S3FileSystem(**s3fs_options)
            filesystem = pyarrow.fs.PyFileSystem(pa.fs.FSSpecHandler(s3fs_instance))

            df_cleaned = make_serializable(df)
            df_arr = df_arr = pa.Table.from_pandas(df_cleaned)

            pq.write_to_dataset(
                df_arr,
                f'{s3_filepath}/{id}.parquet',
                filesystem=filesystem,
                use_dictionary=True,
                compression=compression,
                version="2.4",
            )

        else:
            raise ValueError(f'unknown write_to_disk format: {write_to_disk}')

    def search_time_range(self,
                          Search="COVID",
                          Since="2020-01-01 20:00:00",
                          Until="2020-01-01 21:00:00",
                          job_name=None,
                          tp=None,
                          write_to_disk: Optional[Literal['csv', 'json']] = None,
                          **kwargs):
        tic = time.perf_counter()
        if job_name is None:
            job_name = "search_%s" % Search
        tp = tp or self.tp or TwintPool(is_tor=True)
        logger.info('start search_time_range: %s -> %s', Since, Until)
        t_prev = time.perf_counter()
        for df, t0, t1 in tp._get_term(Search=Search, Since=Since, Until=Until, **kwargs):
            logger.info('hits %s to %s: %s', t0, t1, len(df))
            if self.save_to_neo:
                logger.debug('writing to neo4j')
                hydratetic = time.perf_counter()
                chkd = tp.check_hydrate(df)
                hydratetoc = time.perf_counter()
                logger.info(f'finished checking for hydrate:  {hydratetoc - hydratetic:0.4f} seconds')
                logger.info('search step df shape: %s', df.shape)
                logger.info('chkd shape: %s', chkd.shape)

                res = Neo4jDataAccess(self.neo4j_creds).save_twintdf_to_neo(chkd, job_name, job_id=None)
                # df3 = Neo4jDataAccess(self.debug, self.neo4j_creds).save_df_to_graph(df2, job_name)
                logger.info('wrote to neo4j, # %s' % (len(res) if not (res is None) else 0))
            else:
                res = df
            self._maybe_write_batch(
                res,
                write_to_disk,
                f'{job_name}/{t0}_{t1}',
                write_opts=kwargs.get('write_opts', self.write_opts)
            )
            t_iter = time.perf_counter()
            logger.info(f'finished tp.get_term:  {t_iter - t_prev:0.4f} seconds')
            t_prev = t_iter
            yield res

        toc = time.perf_counter()
        logger.info(f'finished twint loop in:  {toc - tic:0.4f} seconds')
        logger.info('done search_time_range')

