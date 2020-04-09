
from IngestDrugSynonyms import IngestDrugSynonyms
from DrugSynonymDataToNeo4j import DrugSynonymDataToNeo4j

drugSynonym = IngestDrugSynonyms()
drugSynonym.auto_get_and_clean_data()

neo4jBridge = DrugSynonymDataToNeo4j()
# neo4jBridge.upload_drugs_and_synonyms(drugSynonym.drug_vocab)