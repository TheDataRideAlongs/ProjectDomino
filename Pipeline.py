from prefect import Flow, Client, task
from prefect.tasks.shell import ShellTask
import arrow, ast, graphistry, json, os, pprint
import pandas as pd
import numpy as np
from pathlib import Path
from modules.FirehoseJob import FirehoseJob
from datetime import timedelta, datetime
from prefect.schedules import IntervalSchedule
import prefect
from prefect.engine.signals import ENDRUN
from prefect.engine.state import Skipped

@task(log_stdout=True, skip_on_upstream_skip=True)
def load_creds():
    with open('twittercreds.json') as json_file:
        creds = json.load(json_file)
    print([{k: "zzz" for k in creds[0].keys()}])
    return creds

@task(log_stdout=True, skip_on_upstream_skip=True)
def load_path():
    data_dirs = ['COVID-19-TweetIDs/2020-01', 'COVID-19-TweetIDs/2020-02', 'COVID-19-TweetIDs/2020-03']

    timestamp = None
    if 'backfill_timestamp' in prefect.context:
        timestamp = arrow.get(prefect.context['backfill_timestamp'])
    else:
        timestamp = prefect.context['scheduled_start_time']
    print('TIMESTAMP = ', timestamp)
    suffix = timestamp.strftime('%Y-%m-%d-%H')

    for data_dir in data_dirs:
        if os.path.isdir(data_dir):
            for path in Path(data_dir).iterdir():
                if path.name.endswith('{}.txt'.format(suffix)):
                    print(path)
                    return str(path)
        else:
            print('WARNING: not a dir', data_dir)
    # TODO: (wzy) Figure out how to cancel this gracefully
    raise ENDRUN(state=Skipped())

@task(log_stdout=True, skip_on_upstream_skip=True)
def clean_timeline_tweets(pdf):
    return pdf.rename(columns={'id': 'status_id', 'id_str': 'status_id_str'})

@task(log_stdout=True, skip_on_upstream_skip=True)
def clean_datetimes(pdf):
    print('cleaning datetimes...')
    pdf = pdf.assign(created_at=pd.to_datetime(pdf['created_at']))
    pdf = pdf.assign(created_date=pdf['created_at'].apply(lambda dt: dt.timestamp()))
    print('   ...cleaned')
    return pdf

#some reason always False
#this seems to match full_text[:2] == 'RT'
@task(log_stdout=True, skip_on_upstream_skip=True)
def clean_retweeted(pdf):
    return pdf.assign(retweeted=pdf['retweeted_status'] != 'None')

def update_to_type(row):
    if row['is_quote_status']:
        return 'retweet_quote'
    if row['retweeted']:
        return 'retweet'
    if row['in_reply_to_status_id'] is not None and row['in_reply_to_status_id'] > 0:
        return 'reply'
    return 'original'

@task(log_stdout=True, skip_on_upstream_skip=True)
def tag_status_type(pdf):
    ##only materialize required fields..
    print('tagging status...')
    pdf2 = pdf\
        .assign(status_type=pdf[['is_quote_status', 'retweeted', 'in_reply_to_status_id']].apply(update_to_type, axis=1))
    print('   ...tagged')
    return pdf2

def try_load(s):
    try:
        out = ast.literal_eval(s)
        return {
            k if type(k) == str else str(k): out[k]
            for k in out.keys()
        }
    except:
        if s != 0.0:
            print('bad s', s)
        return {}

def flatten_status_col(pdf, col, status_type, prefix):
    print('flattening %s...' % col)
    print('    ', pdf.columns)
    #retweet_status -> hash -> lookup json for hash -> pull out id/created_at/user_id
    pdf_hashed = pdf.assign(hashed=pdf[col].apply(hash))
    retweets = pdf_hashed[ pdf_hashed['status_type'] == status_type ][['hashed', col]]\
        .drop_duplicates('hashed').reset_index(drop=True)
    retweets_flattened = pd.io.json.json_normalize(
        retweets[col].replace("(").replace(")")\
            .apply(try_load))
    print('   ... fixing dates')
    created_at_datetime = pd.to_datetime(retweets_flattened['created_at'])
    created_at = np.full_like(created_at_datetime, np.nan, dtype=np.float64)
    created_at[created_at_datetime.notnull()] = created_at_datetime[created_at_datetime.notnull()].apply(lambda dt: dt.timestamp())
    retweets_flattened = retweets_flattened.assign(
        created_at = created_at,
        user_id = retweets_flattened['user.id'])
    retweets = retweets[['hashed']]\
        .assign(**{
            prefix + c: retweets_flattened[c]
            for c in retweets_flattened if c in ['id', 'created_at', 'user_id']
        })
    print('   ... remerging')
    pdf_with_flat_retweets = pdf_hashed.merge(retweets, on='hashed', how='left').drop(columns='hashed')
    print('   ...flattened', pdf_with_flat_retweets.shape)
    return pdf_with_flat_retweets

@task(log_stdout=True, skip_on_upstream_skip=True)
def flatten_retweets(pdf):
    print('flattening retweets...')
    pdf2 = flatten_status_col(pdf, 'retweeted_status', 'retweet', 'retweet_')
    print('   ...flattened', pdf2.shape)
    return pdf2

@task(log_stdout=True, skip_on_upstream_skip=True)
def flatten_quotes(pdf):
    print('flattening quotes...')
    pdf2 = flatten_status_col(pdf, 'quoted_status', 'retweet_quote', 'quote_')
    print('   ...flattened', pdf2.shape)
    return pdf2

@task(log_stdout=True, skip_on_upstream_skip=True)
def flatten_users(pdf):
    print('flattening users')
    pdf_user_cols = pd.io.json.json_normalize(pdf['user'].replace("(").replace(")").apply(ast.literal_eval))
    pdf2 = pdf.assign(**{
        'user_' + c: pdf_user_cols[c]
        for c in pdf_user_cols if c in [
            'id', 'screen_name', 'created_at', 'followers_count', 'friends_count', 'favourites_count',
            'utc_offset', 'time_zone', 'verified', 'statuses_count', 'profile_image_url',
            'name', 'description'
        ]})
    print('   ... fixing dates')
    pdf2 = pdf2.assign(user_created_at=pd.to_datetime(pdf2['user_created_at']).apply(lambda dt: dt.timestamp()))
    print('   ...flattened')
    return pdf2

@task(log_stdout=True, skip_on_upstream_skip=True)
def load_tweets(creds, path):
    print(path)
    fh = FirehoseJob(creds, PARQUET_SAMPLE_RATE_TIME_S=30)
    cnt = 0
    data = []
    for arr in fh.process_id_file(path, job_name="500m_COVID-REHYDRATE"):
        data.append(arr.to_pandas())
        print('{}/{}'.format(len(data), len(arr)))
        cnt += len(arr)
        print('TOTAL: ' + str(cnt))
    data = pd.concat(data, ignore_index=True, sort=False)
    if len(data) == 0:
        raise ENDRUN(state=Skipped())
    return data

@task(log_stdout=True, skip_on_upstream_skip=True)
def sample(tweets):
    print('responses shape', tweets.shape)
    print(tweets.columns)
    print(tweets.sample(5))

schedule = IntervalSchedule(
    # start_date=datetime(2020, 1, 20),
    # interval=timedelta(hours=1),
    start_date=datetime.now() + timedelta(seconds=1),
    interval=timedelta(hours=1),
)

with Flow("Rehydration Pipeline", schedule=schedule) as flow:
    creds = load_creds()
    path_list = load_path()
    tweets = load_tweets(creds, path_list)
    tweets = clean_timeline_tweets(tweets)
    tweets = clean_datetimes(tweets)
    tweets = clean_retweeted(tweets)
    tweets = tag_status_type(tweets)
    tweets = flatten_retweets(tweets)
    tweets = flatten_quotes(tweets)
    tweets = flatten_users(tweets)

    sample(tweets)

LOCAL_MODE = True

if LOCAL_MODE:
    with prefect.context(
        backfill_timestamp=datetime(2020, 2, 6, 9),
        ):
        flow.run()
else:
    flow.register(project_name="rehydrate")
    flow.run_agent()
