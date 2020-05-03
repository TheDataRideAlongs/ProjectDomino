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
from six.moves import urllib
import re
from collections import Counter
import operator

logging.getLogger().setLevel(logging.ERROR) #DEBUG, INFO, WARNING, ERROR, CRITICAL


async def fetch(url):
    async with ClientSession() as session:
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    return await resp.text()
                else:
                    return ""

        except Exception as ex:
            return ""

async def fact_checker(search_query):
    page_number = 1
    size= 10000
    lingo='en'
    search_query= urlencode(search_query)
    data = await fetch("https://factchecktools.googleapis.com/v1alpha1/claims:search?languageCode={}&pageSize={}&pageToken={}&query={}&key={}".format(lingo,size,page_number,search_query,key))
    return json.loads(data)

# speed up: https://stackoverflow.com/questions/21159103/what-kind-of-problems-if-any-would-there-be-combining-asyncio-with-multiproces
def get_words_from_resp(resp):
    title_pattern = re.compile(r'(?:\<title.*?\>)(.*?)(?:\<\/title\>)', re.IGNORECASE) 

    whitespace_pattern = re.compile(r'([\W_]+)')
    response_titles:list = []
    if isinstance(resp,str) and ('title' in resp or "TITLE" in resp):
        title_text_search_result = title_pattern.search(resp)
        if title_text_search_result:
            for word in title_text_search_result.group(1).split(" "):
                response_titles.append(whitespace_pattern.sub('', word.lower()))
    return response_titles

def urlencode(search_query):
        return urllib.parse.quote(str(search_query).encode("utf-8"))




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
            LIMIT 1000
            RETURN neo4j_id, netloc, hostname, full_url, hits
        """)
        top_urls_df.to_csv(file_name)

    tasks = []

    urls = top_urls_df["full_url"][:10]

    loop = asyncio.get_event_loop()
    tasks = asyncio.gather(*[asyncio.ensure_future(fetch(url)) for url in urls])
    list_of_resp:list = loop.run_until_complete(tasks)
    list_of_list_of_keywords:list = [get_words_from_resp(resp) for resp in list_of_resp]

    # from nltk english corpus embedded for speed
    stopwords = ['ourselves', 'hers', 'between', 'yourself', 'but', 'again', 'there', 'about', 'once', 'during', 'out', 'very', 'having', 'with', 'they', 'own', 'an', 'be', 'some', 'for', 'do', 'its', 'yours', 'such', 'into', 'of', 'most', 'itself', 'other', 'off', 'is', 's', 'am', 'or', 'who', 'as', 'from', 'him', 'each', 'the', 'themselves', 'until', 'below', 'are', 'we', 'these', 'your', 'his', 'through', 'don', 'nor', 'me', 'were', 'her', 'more', 'himself', 'this', 'down', 'should', 'our', 'their', 'while', 'above', 'both', 'up', 'to', 'ours', 'had', 'she', 'all', 'no', 'when', 'at', 'any', 'before', 'them', 'same', 'and', 'been', 'have', 'in', 'will', 'on', 'does', 'yourselves', 'then', 'that', 'because', 'what', 'over', 'why', 'so', 'can', 'did', 'not', 'now', 'under', 'he', 'you', 'herself', 'has', 'just', 'where', 'too', 'only', 'myself', 'which', 'those', 'i', 'after', 'few', 'whom', 't', 'being', 'if', 'theirs', 'my', 'against', 'a', 'by', 'doing', 'it', 'how', 'further', 'was', 'here', 'than']

    response_keywords = [keyword for list_of_keywords in list_of_list_of_keywords for keyword in list_of_keywords if keyword not in stopwords and keyword != '' and not keyword.isnumeric()]



    counted_words = Counter(response_keywords)

    maxcount_key  = max(counted_words.items(), key=operator.itemgetter(1))[0]

    threshold:int = int(counted_words[maxcount_key]*0.05)

    #print(threshold)

    red_counted_words = dict(filter(lambda elem: elem[1] > max(threshold,1), counted_words.items()))

    #print(len(red_counted_words))

    queries = asyncio.gather(*[asyncio.ensure_future(fact_checker(keyword[0])) for keyword in red_counted_words.items()])
    query_responses:list = loop.run_until_complete(queries)


    flatten_claims:list = []

    for claim_list in query_responses:
        flatten_claims.extend(claim_list["claims"])

    def print_text(claim_list:list)-> [str]:
        print([claim["text"] for claim in claim_list])

    print(len(flatten_claims))
    print(flatten_claims[0])

    unique_claims = list({v['text']:v for v in flatten_claims}.values())

    print(len(unique_claims))

    print(unique_claims[0])

    # TODO insert URLS
    # TODO insert claims

    loop.close()


