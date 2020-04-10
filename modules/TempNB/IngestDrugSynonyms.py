import requests
import pandas as pd
import re
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
import logging
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger('ds')


class IngestDrugSynonyms():

    def __init__(self,configPath:Path=Path("config.json")):
        self.check_config(configPath)
        self.url_international:str = os.environ["URL_INT"] if isinstance(os.environ["URL_INT"],str) else None
        self.url_USA:str = os.environ["URL_USA"] if isinstance(os.environ["URL_USA"],str) else None
        self.url_drugbank:str = os.environ["URL_DRUGBANK"] if isinstance(os.environ["URL_DRUGBANK"],str) else None
        self.query_keywords:[] = os.environ["QUERY_KEYWORDS"].split(",") if isinstance(os.environ["QUERY_KEYWORDS"],str) else None

    @staticmethod
    def check_config(configPath:Path=Path("config.json")):
        def load_config(configPath:Path):
            with open(configPath) as f:
                config = json.load(f)
                for key in config:
                    os.environ[key] = config[key]
        rel_path:Path = Path(os.path.realpath(__file__)).parents[0] / configPath
        if rel_path.exists:
            load_config(rel_path)
        elif configPath.exists:
            load_config(configPath)
        else:
            logger.warning("Could not load config file from: {}".format(configPath))

    @staticmethod
    def api(query,from_study,to_study,url):
        url = url.format(query,from_study,to_study)
        response = requests.request("GET", url)
        return response.json()

    def api_wrapper(self,query,from_study):
        return self.api(query,from_study,from_study+99,self.url_USA)
    
    def getAllStudiesByQuery(self,query:str) -> list:
        logger.info("> STARTING scraping with '{}' keyword".format(query))
        studies:list = []
        from_study = 1
        temp = self.api_wrapper(query,from_study)
        nstudies = temp['FullStudiesResponse']['NStudiesFound']
        logger.info("> {} studies found by '{}' keyword".format(nstudies,query))
        if nstudies > 0:
            studies = temp['FullStudiesResponse']['FullStudies']
            for study_index in range(from_study+100,nstudies,100):
                temp = self.api_wrapper(query,study_index)
                studies.extend(temp['FullStudiesResponse']['FullStudies'])
        
        return studies
    
    @staticmethod
    def xls_handler(r):
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
    def csvzip_handler(r):
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

    @staticmethod
    def _convert_US_studies(US_studies:dict) -> pd.DataFrame:
        list_of_US_studies:list = []
        for key in US_studies.keys():
            for study in US_studies[key]:
                temp_dict:dict = {}
                
                temp_dict["trial_id"] = study["Study"]["ProtocolSection"]["IdentificationModule"]["NCTId"]
                temp_dict["study_url"] = "https://clinicaltrials.gov/show/" + temp_dict["trial_id"]

                try:
                    temp_dict["intervention"] = study["Study"]["ProtocolSection"]["ArmsInterventionsModule"]["ArmGroupList"]["ArmGroup"][0]["ArmGroupInterventionList"]["ArmGroupInterventionName"][0]
                except:
                    temp_dict["intervention"] = ""
                try:
                    temp_dict["study_type"] = study["Study"]["ProtocolSection"]["DesignModule"]["StudyType"]
                except:
                    temp_dict["study_type"] = ""
                try:
                    temp_dict["target_size"] = study["Study"]["ProtocolSection"]["DesignModule"]["EnrollmentInfo"]["EnrollmentCount"]
                except:
                    temp_dict["target_size"] = ""
                try:
                    if "OfficialTitle" in study["Study"]["ProtocolSection"]["IdentificationModule"].keys():
                        temp_dict["public_title"] = study["Study"]["ProtocolSection"]["IdentificationModule"]["OfficialTitle"]
                    else:
                        temp_dict["public_title"] = study["Study"]["ProtocolSection"]["IdentificationModule"]["BriefTitle"]
                except:
                    temp_dict["public_title"] = ""
                list_of_US_studies.append(temp_dict)
        US_studies_df:pd.DataFrame = pd.DataFrame(list_of_US_studies)
        return US_studies_df

    def _scrapeData(self):
        self.internationalstudies = self.urlToDF(self.url_international,self.xls_handler)
        self.drug_vocab_df = self.urlToDF(self.url_drugbank,self.csvzip_handler)
        self.all_US_studies_by_keyword:dict = {}
        for key in self.query_keywords:
            self.all_US_studies_by_keyword[key] = self.getAllStudiesByQuery(key)
    
    def _filterData(self):
        self.drug_vocab_reduced = self.drug_vocab_df[['Common name', 'Synonyms']]
        self.internationalstudies_reduced = self.internationalstudies[['TrialID', 'Intervention','Study type','web address','Target size', "Public title"]]
        self.internationalstudies_reduced.columns = [col.replace(" ","_").lower() for col in self.internationalstudies_reduced.columns]
        cols_to_replace:dict = {
            "trialid":"trial_id",
            "web_address":"study_url"
            }
        self.internationalstudies_reduced.columns = [cols_to_replace.get(n, n) for n in self.internationalstudies_reduced.columns]

        self.drug_vocab:dict = {}
        for row in self.drug_vocab_reduced.to_dict('records'):
            self.drug_vocab[row['Common name'].lower().strip()] = list(
                                                                        filter(
                                                                            lambda x: x is not None and len(x) >= 3,
                                                                            list(map(lambda x: x.lower().strip() if x.lower().strip() != row['Common name'].lower().strip() else None,
                                                                            row["Synonyms"].split("|"))))) if isinstance(row["Synonyms"],str) else row["Synonyms"]

        self.US_studies_df = self._convert_US_studies(self.all_US_studies_by_keyword)

        self.all_studies_df = pd.concat([self.US_studies_df,self.internationalstudies_reduced],sort=False,ignore_index=True)
        self.all_studies_df.drop_duplicates(subset="trial_id",inplace=True)
        self.all_studies_df.reset_index(drop=True, inplace=True)
        self.all_studies_df.fillna("",inplace=True)
        self.urls:list = list(self.all_studies_df["study_url"])
        logger.info("> {} distinct studies found".format(len(self.all_studies_df)))

    def save_data_to_file(self):
        """Saving data option for debug purposes"""
        logger.warning("Only Use it for debug purposes!!!")
        self.internationalstudies.to_csv("internationalstudies.csv")
        self.drug_vocab_df.to_csv("drug_vocab.csv")
        with open('all_US_studies_by_keyword.json', 'w', encoding='utf-8') as f:
            json.dump(self.all_US_studies_by_keyword, f, ensure_ascii=False, indent=4)

    def auto_get_and_clean_data(self):
        self._scrapeData()
        self._filterData()

    def create_drug_study_links(self):
        drug_vocab = self.drug_vocab


        drugs_and_syms:list = list(drug_vocab.keys())
        drugs_and_syms.extend( item.lower() for key in drug_vocab.keys() if isinstance(drug_vocab[key],list) for item in drug_vocab[key] )
        ids_and_interventions:list = [(row["trial_id"],row["intervention"].lower()) for row in self.all_studies_df.to_dict('records')]

        self.appeared_in_edges:list = [(drug,trial_id) for drug in drugs_and_syms for trial_id,intervention in ids_and_interventions if bool(re.compile(r"\b%s\b" % re.escape(drug)).search(intervention))]
    
    def create_url_study_links(self):
        self.url_points_at_study_edges:list = [(row["study_url"],row["trial_id"]) for row in self.all_studies_df.to_dict('records')]