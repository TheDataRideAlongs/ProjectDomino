#!/usr/bin/env python
# coding: utf-8


import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO) #DEBUG, INFO, WARNING, ERROR, CRITICAL



import json, os, pandas as pd, pendulum
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

stride_sec = int(os.environ['DOMINO_STRIDE_SEC']) if env_non_empty('DOMINO_STRIDE_SEC') else 30
delay_sec = int(os.environ['DOMINO_DELAY_SEC']) if env_non_empty('DOMINO_DELAY_SEC') else 60
job_name = os.environ['DOMINO_JOB_NAME'] if env_non_empty('DOMINO_JOB_NAME') else "covid"
search = os.environ['DOMINO_SEARCH'] if env_non_empty('DOMINO_SEARCH') else "covid OR corona OR virus OR pandemic"

@task(log_stdout=True, skip_on_upstream_skip=True)
def run_stream():

    start = context.scheduled_start_time - timedelta(seconds=delay_sec + stride_sec)
    current = start + timedelta(seconds=stride_sec)
    #start = datetime.strptime("2020-10-06 22:10:00", "%Y-%m-%d %H:%M:%S")
    #current = datetime.strptime("2020-10-10 16:08:00", "%Y-%m-%d %H:%M:%S")
    #current = datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
    #2020-10-10 16:07:30
    #2020-10-11 06:29:00 to 2020-10-11 06:29:30:
    #2020-10-11 18:45:00 to 2020-10-11 18:45:30:
    #2020-10-05 17:00:30-2020-10-05 17:01:00
    # 2020-10-06 22:10:00 to 2020-10-06 22:10:30:
    tp = TwintPool(is_tor=True)
    fh = FirehoseJob(PARQUET_SAMPLE_RATE_TIME_S=30, save_to_neo=False, writers={}, write_to_disk='json')
    
    try:
        for df in fh.search_time_range(
            tp=tp,
            Search=search,
            Since=datetime.strftime(start, "%Y-%m-%d %H:%M:%S"),
            Until=datetime.strftime(current, "%Y-%m-%d %H:%M:%S"),
            job_name=job_name,
            Limit=10000000,
            stride_sec=stride_sec
        ):
            logger.info('got: %s', len(df) if not (df is None) else 'None')
            logger.info('proceed to next df')
    except Exception as e:
        logger.error("job exception", exc_info=True)
        raise e
    logger.info("job finished")


schedule_opts = {
    'interval': timedelta(seconds=stride_sec)
}
print(f'Schedule options: {schedule_opts}')

schedule = IntervalSchedule(**schedule_opts)
storage = S3(bucket=S3_BUCKET)

#with Flow("covid-19 stream-single") as flow:
#with Flow("covid-19 stream", storage=storage, schedule=schedule) as flow:
with Flow(f"{job_name} stream", schedule=schedule) as flow:
    run_stream()
flow.run()

