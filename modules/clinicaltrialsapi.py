#!/usr/bin/env python
# coding: utf-8

# In[16]:


import requests
import pandas as pd
import json
import sys
from pathlib import Path
import xlrd
import csv
import os
import tempfile


# In[23]:


pd.set_option('display.max_colwidth', -1)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


# In[17]:


from IPython.core.display import display, HTML
display(HTML("<style>.container { width:100% !important; }</style>"))


# In[41]:


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


# In[113]:


def api(query,from_study,to_study):
    url = "https://www.clinicaltrials.gov/api/query/full_studies?expr={}&min_rnk={}&max_rnk={}&fmt=json".format(query,from_study,to_study)
    response = requests.request("GET", url)
    return response.json()


# In[75]:


def apiWrapper(query,from_study):
    return api(query,from_study,from_study+99)


# In[100]:


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


# In[111]:


all_studies_by_keyword:dict = {}
queries:list = ["covid-19 Lopinavir","covid-19 Ritonavir" ,"covid-19 chloroquine"]

for key in queries:
    all_studies_by_keyword[key] = getAllStudiesByQuery(key)


# In[107]:


print(all_studies_by_keyword.keys())
print([len(all_studies_by_keyword[key]) for key in all_studies_by_keyword.keys()])


# In[89]:



temp = api(query="covid-19",from_study=201,to_study=300)


# In[90]:


# dict_keys(['APIVrs', 'DataVrs', 'Expression', 'NStudiesAvail', 'NStudiesFound', 'MinRank', 'MaxRank', 'NStudiesReturned', 'FullStudies'])
print(temp['FullStudiesResponse']['NStudiesFound'])
print(temp['FullStudiesResponse']['FullStudies'][-1]["Rank"])


# ## api query

# In[114]:


api(query="covid-19 Lopinavir",from_study=1,to_study=from_study+99)


# In[28]:


df =  pd.DataFrame(api(query="coronavirus",from_study=101,to_study=200))


# In[11]:


df


# ## who database

# In[29]:


url = "https://www.who.int/docs/default-source/coronaviruse/covid-19-trials.xls"


# In[30]:


internationalstudies = xlsUrlToDF(url)


# In[31]:


internationalstudies


# In[34]:


internationalstudies.count()


# In[ ]:




