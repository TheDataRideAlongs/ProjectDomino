#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


pd.set_option('display.max_colwidth', -1)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


# In[17]:


get_ipython().system(' wget -O drug_vocab.csv.zip https://www.drugbank.ca/releases/5-1-5/downloads/all-drugbank-vocabulary')


# In[18]:


vocab=pd.read_csv("drug_vocab.csv.zip")


# In[21]:


vocab.head(20)


# In[ ]:





# In[ ]:





# In[ ]:




