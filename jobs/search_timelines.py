#!/usr/bin/env python
# coding: utf-8


import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO) #DEBUG, INFO, WARNING, ERROR, CRITICAL



import json, os, pandas as pd, pendulum, sys
from ProjectDomino.Neo4jDataAccess import Neo4jDataAccess
from ProjectDomino.FirehoseJob import FirehoseJob
from ProjectDomino.TwintPool import TwintPool
from prefect.environments.storage import S3
from prefect import context, Flow, task
from prefect.schedules import IntervalSchedule
from datetime import timedelta, datetime
from prefect.engine.executors import DaskExecutor


S3_BUCKET = "wzy-project-domino"

pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

def env_non_empty(x: str):
    return x in os.environ and os.environ[x]

def str_to_bool (x: str):
    if x in ['True', 'true', '1', 'TRUE']:
        return True
    elif x in ['False', 'false', '0', 'FALSE']:
        return False
    else:
        raise ValueError('Cannot convert to bool: ' + x)

stride_sec = int(os.environ['DOMINO_STRIDE_SEC']) if env_non_empty('DOMINO_STRIDE_SEC') else 30
job_name = os.environ['DOMINO_JOB_NAME'] if env_non_empty('DOMINO_JOB_NAME') else "covid"
write_format = os.environ['DOMINO_WRITE_FORMAT'] if env_non_empty('DOMINO_WRITE_FORMAT') else None
fetch_profiles = str_to_bool(os.environ['DOMINO_FETCH_PROFILES']) if env_non_empty('DOMINO_FETCH_PROFILES') else False
usernames_raw = os.environ['DOMINO_USERNAMES'] if env_non_empty('DOMINO_USERNAMES') else None
if usernames_raw is None:
    raise ValueError('DOMINO_USERNAMES is not set, expected comma-delimited str')
usernames = usernames_raw.split(',')
usernames = [ x for x in usernames if len(x) > 0 ]

if write_format == 'parquet_s3':
    s3_filepath = os.environ['DOMINO_S3_FILEPATH'] if env_non_empty('DOMINO_S3_FILEPATH') else None
    AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
    AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
    compression = os.environ['DOMINO_COMPRESSION'] if env_non_empty('DOMINO_COMPRESSION') else 'snappy'

output_path = f'/output/{job_name}'
os.makedirs(f'{output_path}/tweets', exist_ok=True)
os.makedirs(f'{output_path}/profiles', exist_ok=True)
os.makedirs(f'{output_path}/timelines', exist_ok=True)


# FIXME unsafe when distributed
usernames_queue = usernames.copy()
pending = 0

@task(log_stdout=True, skip_on_upstream_skip=True, max_retries=3, retry_delay=timedelta(seconds=30))
def run_stream():

    global pending

    if len(usernames_queue) == 0 and pending == 0:
        logger.info(f'Successfully processed all usernames ({len(usernames)}), exiting')
        sys.exit(0)

    if len(usernames_queue) == 0:
        logger.info(f'No more usernames to process, but {pending} jobs are still pending')
        return

    pending += 1
    username = usernames_queue.pop(0)

    try:

        tp = TwintPool(is_tor=True)
        fh = FirehoseJob(
            PARQUET_SAMPLE_RATE_TIME_S=30,
            save_to_neo=False,
            tp=tp,
            writers={},
            write_to_disk=write_format,
            write_opts=(
                {
                    's3_filepath': s3_filepath,
                    's3fs_options': {
                        'key': AWS_ACCESS_KEY_ID,
                        'secret': AWS_SECRET_ACCESS_KEY
                    },
                    'compression': compression
                }
                if write_format == 'parquet_s3' else
                {}
            )
        )
        
        try:
            for df in fh.get_timelines(
                usernames=[username],
                job_name=job_name,
                fetch_profiles = fetch_profiles
            ):
                print('got: %s', df.shape if df is not None else 'None')
        except Exception as e:
            logger.error("job exception", exc_info=True)
            raise e
    except:
        logger.error("task exception, reinserting user", exc_info=True)
        usernames_queue.insert(0, username)
    pending -= 1
    print("task finished")


schedule_opts = {
    'interval': timedelta(seconds=stride_sec),
    'start_date': pendulum.parse('2019-01-01 00:00:00')
}
logger.info(f'Schedule options: {schedule_opts}')
logger.info(f'Task settings: stride_sec={stride_sec}')

schedule = IntervalSchedule(**schedule_opts)
storage = S3(bucket=S3_BUCKET)

#with Flow("covid-19 stream-single") as flow:
#with Flow("covid-19 stream", storage=storage, schedule=schedule) as flow:
with Flow(f"{job_name} stream", schedule=schedule) as flow:
    run_stream()
flow.run()

