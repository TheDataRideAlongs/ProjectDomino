import aiohttp
import asyncio
from aiohttp import ClientSession
import json, logging
import pandas as pd
import time
from modules.Neo4jDataAccess import Neo4jDataAccess
from progress.bar import Bar
import os

logging.getLogger().setLevel(logging.ERROR) #DEBUG, INFO, WARNING, ERROR, CRITICAL


async def fetch(url):
    async with ClientSession() as session:
        try:
            async with session.get(url) as resp:
                # print(await resp.text())
                return resp.status
        except:
            return "Error"


if __name__ == "__main__":

    creds = []
    with open('localKeys/twittercreds.json') as json_file:
        creds = json.load(json_file)
        
    neo4j_creds = None
    with open('localKeys/neo4jcreds.json') as json_file:
        neo4j_creds = json.load(json_file)

    file_name:str = "example_urls.csv"

    if os.path.exists(file_name):
        top_urls_df = pd.read_csv(file_name)
    else:
        top_urls_df = Neo4jDataAccess(neo4j_creds=neo4j_creds).get_from_neo("""
        
            MATCH (a:Url)<-[r]-()
            WITH a.netloc as netloc, a.hostname as hostname, a.full_url as full_url, count(r) as hits
            ORDER BY hits DESC
            LIMIT 100000
            RETURN netloc, hostname, full_url, hits
        """)
        top_urls_df.to_csv(file_name)

    print('df len', len(top_urls_df))


    tasks = []
    start = time.time()
    urls = top_urls_df["full_url"][:]

    loop = asyncio.get_event_loop()
    tasks = asyncio.gather(*[asyncio.ensure_future(fetch(url)) for url in urls])
    responses:list = loop.run_until_complete(tasks)
    
    loop.close()


    print(time.time() - start)
    print(responses)