
from modules.IngestDrugSynonyms import IngestDrugSynonyms
from modules.DrugSynonymDataToNeo4j import DrugSynonymDataToNeo4j
import logging

logging.basicConfig(format='>>> %(message)s', level=logging.INFO)

drugSynonym = IngestDrugSynonyms()
drugSynonym.auto_get_and_clean_data()

neo4jBridge = DrugSynonymDataToNeo4j()
neo4jBridge.upload_drugs_and_synonyms(drugSynonym.drug_vocab)
neo4jBridge.upload_studies(drugSynonym.all_studies_df)