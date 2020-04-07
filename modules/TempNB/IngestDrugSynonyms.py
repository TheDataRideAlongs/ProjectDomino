import requests
import pandas as pd
import json
import sys
from pathlib import Path
import xlrd
import csv
import os
import tempfile
import numpy as np
from typing import Optional
from functools import partial

class DrugSynonymIngest():

    def __init__(self,configPath:Path=Path("config.json")):
        self.checkConfig(configPath)
        self.url_international:str = os.environ["URL_INT"] if isinstance(os.environ["URL_INT"],str) else None
        self.url_USA:str = os.environ["URL_USA"] if isinstance(os.environ["URL_USA"],str) else None
        self.url_drugbank:str = os.environ["URL_DRUGBANK"] if isinstance(os.environ["URL_DRUGBANK"],str) else None
        self.query_keywords:[] = os.environ["QUERY_KEYWORDS"].split(",") if isinstance(os.environ["QUERY_KEYWORDS"],str) else None

    @staticmethod
    def checkConfig(configPath:Path=Path("config.json")):
        if configPath.exists:
            with open(configPath) as f:
                config = json.load(f)
                for key in config:
                    os.environ[key] = config[key]

    @staticmethod
    def api(query,from_study,to_study,url):
        url = url.format(query,from_study,to_study)
        response = requests.request("GET", url)
        return response.json()

    def apiWrapper(self,query,from_study):
        return self.api(query,from_study,from_study+99,self.url_USA)
    
    def getAllStudiesByQuery(self,query:str) -> list:
        studies:list = []
        from_study = 1
        temp = self.apiWrapper(query,from_study)
        nstudies = temp['FullStudiesResponse']['NStudiesFound']
        print("> {} studies found by '{}' keyword".format(nstudies,query))
        if nstudies > 0:
            studies = temp['FullStudiesResponse']['FullStudies']
            for study_index in range(from_study+100,nstudies,100):
                temp = self.apiWrapper(query,study_index)
                studies.extend(temp['FullStudiesResponse']['FullStudies'])
        
        return studies
    
    @staticmethod
    def xlsHandler(r):
        df = pd.DataFrame()
        with tempfile.NamedTemporaryFile("wb") as xls_file:
            xls_file.write(r.content)
        
            try:
                book = xlrd.open_workbook(xls_file.name,encoding_override="utf-8")  
            except:
                book = xlrd.open_workbook(xls_file.name,encoding_override="cp1251")

            sh = book.sheet_by_index(0)
            with tempfile.NamedTemporaryFile("w") as csv_file:
                wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)

                for rownum in range(sh.nrows):
                    wr.writerow(sh.row_values(rownum))
                df = pd.read_csv(csv_file.name)
                csv_file.close()

            xls_file.close()
        return df

    @staticmethod
    def csvZipHandler(r):
        df = pd.DataFrame()
        with tempfile.NamedTemporaryFile("wb",suffix='.csv.zip') as file:
            file.write(r.content)
            df = pd.read_csv(file.name)
            file.close()
        return df

    @staticmethod
    def urlToDF(url:str,respHandler) -> pd.DataFrame:
        r = requests.get(url, allow_redirects=True)
        return respHandler(r)

    def scrapeData(self):
        self.internationalstudies = self.urlToDF(self.url_international,self.xlsHandler)
        self.drug_vocab_df = self.urlToDF(self.url_drugbank,self.csvZipHandler)
        self.all_US_studies_by_keyword:dict = {}
        for key in self.query_keywords:
            self.all_US_studies_by_keyword[key] = self.getAllStudiesByQuery(key)
    
    def filterData(self):
        self.drug_vocab_reduced = self.drug_vocab_df[['Common name', 'Synonyms']]
        self.drug_vocab:dict = {}
        for index, row in self.drug_vocab_reduced.iterrows():
            self.drug_vocab[row['Common name']] = row["Synonyms"].split("|") if isinstance(row["Synonyms"],str) else row["Synonyms"]

    def saveDataToFile(self):
        """Saving data option for debug purposes"""
        print("Only Use it for debug purposes")
        self.internationalstudies.to_csv("internationalstudies.csv")
        self.drug_vocab.to_csv("drug_vocab.csv")
        with open('all_US_studies_by_keyword.json', 'w', encoding='utf-8') as f:
            json.dump(self.all_US_studies_by_keyword, f, ensure_ascii=False, indent=4)
