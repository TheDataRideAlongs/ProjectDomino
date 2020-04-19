import aiohttp
import asyncio
from aiohttp import ClientSession
import json, logging
import pandas as pd
import time
from modules.Neo4jDataAccess import Neo4jDataAccess
from progress.bar import Bar
import os
from bs4 import BeautifulSoup as soup
import requests
from six.moves import urllib

logging.getLogger().setLevel(logging.ERROR) #DEBUG, INFO, WARNING, ERROR, CRITICAL


async def fetch(url):
    async with ClientSession() as session:
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    return await resp.read()
                else:
                    return ""

        except Exception as ex:
            return ""


if __name__ == "__main__":

    creds = []
    with open('localKeys/twittercreds.json') as json_file:
        creds = json.load(json_file)
        
    neo4j_creds = None
    with open('localKeys/neo4jcreds.json') as json_file:
        neo4j_creds = json.load(json_file)

    key = []
    with open('localKeys/factkeys.json') as json_file:
        key = json.load(json_file)

    file_name:str = "example_urls.csv"

    if os.path.exists(file_name):
        top_urls_df = pd.read_csv(file_name)
    else:
        top_urls_df = Neo4jDataAccess(neo4j_creds=neo4j_creds).get_from_neo("""
        
            MATCH (a:Url)<-[r]-()
            WITH ID(a) as neo4j_id, a.netloc as netloc, a.hostname as hostname, a.full_url as full_url, count(r) as hits
            ORDER BY hits DESC
            LIMIT 100000
            RETURN neo4j_id, netloc, hostname, full_url, hits
        """)
        top_urls_df.to_csv(file_name)

    tasks = []

    urls = top_urls_df["full_url"][:1]

    print(urls[0])

    loop = asyncio.get_event_loop()
    tasks = asyncio.gather(*[asyncio.ensure_future(fetch(url)) for url in urls])
    responses:list = loop.run_until_complete(tasks)
    
    response_titles = [soup(str(soup(resp, 'html.parser').title),"html.parser").text for resp in responses]

    print(response_titles)

    loop.close()

    def urlencode(search_query):
        return urllib.parse.quote(str(search_query).encode("utf-8"))

    def fact_checker(search_query):
        page_number = 1
        size= 10000
        lingo='en'
        search_query= urlencode(search_query)
        url = requests.get("https://factchecktools.googleapis.com/v1alpha1/claims:search?languageCode={}&pageSize={}&pageToken={}&query={}&key={}".format(lingo,size,page_number,search_query,key))
        return url.json()


