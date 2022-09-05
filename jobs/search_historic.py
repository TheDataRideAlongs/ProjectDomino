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
historic_stride_sec = int(os.environ['DOMINO_HISTORIC_STRIDE_SEC']) if env_non_empty('DOMINO_HISTORIC_STRIDE_SEC') else 60 * 60 * 24
twint_stride_sec = int(os.environ['DOMINO_TWINT_STRIDE_SEC']) if env_non_empty('DOMINO_TWINT_STRIDE_SEC') else round(historic_stride_sec/2)
delay_sec = int(os.environ['DOMINO_DELAY_SEC']) if env_non_empty('DOMINO_DELAY_SEC') else 60
job_name = os.environ['DOMINO_JOB_NAME'] if env_non_empty('DOMINO_JOB_NAME') else "covid"
start_date = pendulum.parse(os.environ['DOMINO_START_DATE']) if env_non_empty('DOMINO_START_DATE') else datetime.datetime.now() - datetime.timedelta(days=365)
search = os.environ['DOMINO_SEARCH'] if env_non_empty('DOMINO_SEARCH') else "covid OR corona OR virus OR pandemic"
write_format = os.environ['DOMINO_WRITE_FORMAT'] if env_non_empty('DOMINO_WRITE_FORMAT') else None

if write_format == 'parquet_s3':
    s3_filepath = os.environ['DOMINO_S3_FILEPATH'] if env_non_empty('DOMINO_S3_FILEPATH') else None
    AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
    AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
    compression = os.environ['DOMINO_COMPRESSION'] if env_non_empty('DOMINO_COMPRESSION') else 'snappy'

output_path = f'/output/{job_name}'
os.makedirs(output_path, exist_ok=True)


# FIXME unsafe when distributed
task_num = -1

@task(log_stdout=True, skip_on_upstream_skip=True)
def run_stream():

    global task_num
    task_num = task_num + 1

    start = start_date + timedelta(seconds=task_num * historic_stride_sec)
    current = start + timedelta(seconds=historic_stride_sec)
    print('------------------------')
    print('task %s with start %s: %s to %s', task_num, start_date, start, current)
    #start = datetime.strptime("2020-10-06 22:10:00", "%Y-%m-%d %H:%M:%S")
    #current = datetime.strptime("2020-10-10 16:08:00", "%Y-%m-%d %H:%M:%S")
    #current = datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
    #2020-10-10 16:07:30
    #2020-10-11 06:29:00 to 2020-10-11 06:29:30:
    #2020-10-11 18:45:00 to 2020-10-11 18:45:30:
    #2020-10-05 17:00:30-2020-10-05 17:01:00
    # 2020-10-06 22:10:00 to 2020-10-06 22:10:30:
    tp = TwintPool(is_tor=True)
    fh = FirehoseJob(
        PARQUET_SAMPLE_RATE_TIME_S=30,
        save_to_neo=False,
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
        for df in fh.search_time_range(
            tp=tp,
            Search=search,
            Since=datetime.strftime(start, "%Y-%m-%d %H:%M:%S"),
            Until=datetime.strftime(current, "%Y-%m-%d %H:%M:%S"),
            job_name=job_name,
            Limit=10000000,
            stride_sec=twint_stride_sec
        ):
            print('got: %s', df.shape if df is not None else 'None')
    except Exception as e:
        logger.error("job exception", exc_info=True)
        raise e
    print("task finished")


schedule_opts = {
    'interval': timedelta(seconds=stride_sec),
    'start_date': pendulum.parse('2019-01-01 00:00:00')
}
logger.info(f'Schedule options: {schedule_opts}')
logger.info(f'Task settings: stride_sec={stride_sec}, \
        historic_stride_sec={historic_stride_sec}, \
        twint_stride_sec={twint_stride_sec} \
        start_date={start_date}, \
        search={search}')

schedule = IntervalSchedule(**schedule_opts)
storage = S3(bucket=S3_BUCKET)

#with Flow("covid-19 stream-single") as flow:
#with Flow("covid-19 stream", storage=storage, schedule=schedule) as flow:
with Flow(f"{job_name} stream", schedule=schedule) as flow:
    run_stream()
flow.run()

