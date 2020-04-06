import requests
import pandas as pd
import json
import sys
from pathlib import Path
import xlrd
import csv
import os
import tempfile


def api(query,from_study,to_study):
    url = "https://www.clinicaltrials.gov/api/query/full_studies?expr={}&min_rnk={}&max_rnk={}&fmt=json".format(query,from_study,to_study)
    response = requests.request("GET", url)
    return response.json()

def apiWrapper(query,from_study):
    return api(query,from_study,from_study+99)

def xlsUrlToDF(url:str) -> pd.DataFrame:
    r = requests.get(url, allow_redirects=True)
    df = pd.DataFrame()
    with tempfile.NamedTemporaryFile("wb") as xls_file:
        xls_file.write(r.content)
    
        try:
            book = xlrd.open_workbook(xls_file.name,encoding_override="cp1251")  
        except:
            book = xlrd.open_workbook(file)

        sh = book.sheet_by_index(0)
        with tempfile.NamedTemporaryFile("w") as csv_file:
            wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)

            for rownum in range(sh.nrows):
                wr.writerow(sh.row_values(rownum))
            df = pd.read_csv(csv_file.name)
            csv_file.close()

        xls_file.close()
    return df

def getAllStudiesByQuery(query:str) -> list:
    studies:list = []
    from_study = 1
    temp = apiWrapper(query,from_study)
    nstudies = temp['FullStudiesResponse']['NStudiesFound']
    print("> {} studies found by '{}' keyword".format(nstudies,query))
    if nstudies > 0:
        studies = temp['FullStudiesResponse']['FullStudies']
        for study_index in range(from_study+100,nstudies,100):
            temp = apiWrapper(query,study_index)
            studies.extend(temp['FullStudiesResponse']['FullStudies'])
    
    return studies

url = "https://www.who.int/docs/default-source/coronaviruse/covid-19-trials.xls"
internationalstudies = xlsUrlToDF(url)