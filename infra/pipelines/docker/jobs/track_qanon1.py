#!/usr/bin/env python
# coding: utf-8


import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO) #DEBUG, INFO, WARNING, ERROR, CRITICAL



import json, pandas as pd
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


def get_creds():
    neo4j_creds = None
    with open('/secrets/neo4jcreds.json') as json_file:
        neo4j_creds = json.load(json_file)
    return neo4j_creds

@task(log_stdout=True, skip_on_upstream_skip=True)
def run_stream():
    creds = get_creds()

    start = context.scheduled_start_time - timedelta(seconds=60)
    current = start + timedelta(seconds=30)
    #start = datetime.strptime("2020-10-06 22:10:00", "%Y-%m-%d %H:%M:%S")
    #current = datetime.strptime("2020-10-10 16:08:00", "%Y-%m-%d %H:%M:%S")
    #current = datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
    #2020-10-10 16:07:30
    #2020-10-11 06:29:00 to 2020-10-11 06:29:30:
    #2020-10-11 18:45:00 to 2020-10-11 18:45:30:
    #2020-10-05 17:00:30-2020-10-05 17:01:00
    # 2020-10-06 22:10:00 to 2020-10-06 22:10:30:
    terms = [ 
            "17anon",
            "adrenochromeharvesting",
            "agenda21",
            "C0vid-19",
            "cannibalclub",
            "exposepedogate",
            "fake virus",
            "great awakening",
            "HisNameWasSethRich",
            "outofshadows",
            "Patriot Defender",
            "pedovore",
            "pedowood",
            "qanon",
            "qanonmap",
            "qanons",
            "qanonymous",
            "qanontruth",
            "qanon2020",
            "qdrop",
            "qanondrops",
            "qanonposts",
            "qanonpub",
            "qsentme",
            "QANONWORLDWIDE",
            "QANON_WORLDWIDE",
            "QAnonUS",
            "QAnonUSA",
            "QAnonUK",
            "QAnonCanada",
            "greatawakening",
            "thegreateawakening",
            "qmap_pub",
            "Rothshilds", 
            "save the children",
            "saveourchildren",
            "savethechildren",
            "saveourplanet",
            "sheepnomore",
            "silentnomore",
            "taketheoath",
            "digitalsoldiers",
            "trumprussiamatrix",
            "trusttheplan",
            "weareq",
            "weareallq",
            "WWG1WGA",
            "wwg",
            "wga",
            "wgaworldwide",
            "wwg1wgaworldwide",
            "wwg1wga_worldwide"
    ]

    search_term = " OR ".join(['"' + t + '"' for t in terms[:20]])
    logger.info('Using :20: %s', search_term)


    tp = TwintPool(is_tor=True)
    fh = FirehoseJob(neo4j_creds=creds, PARQUET_SAMPLE_RATE_TIME_S=30, save_to_neo=True, writers={})
    try:
        search = search_term
        job_name = "qanon"
        limit = 10000000
        for df in fh.search_time_range(tp=tp, Search=search, Since=datetime.strftime(start, "%Y-%m-%d %H:%M:%S"), Until=datetime.strftime(current, "%Y-%m-%d %H:%M:%S"), job_name=job_name, Limit=10000000, stride_sec=30):
            logger.info('got: %s', len(df) if not (df is None) else 'None')
            logger.info('proceed to next df')
    except Exception as e:
        logger.error("job exception", exc_info=True)
        raise e
    logger.info("job finished")

schedule = IntervalSchedule(
    interval=timedelta(seconds=30),
)
storage = S3(bucket=S3_BUCKET)

#with Flow("covid-19 stream-single") as flow:
#with Flow("covid-19 stream", storage=storage, schedule=schedule) as flow:
with Flow("qanon stream", schedule=schedule) as flow:
    run_stream()
flow.run()

