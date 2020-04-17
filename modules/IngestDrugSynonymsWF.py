
from IngestDrugSynonyms import IngestDrugSynonyms
from DrugSynonymDataToNeo4j import DrugSynonymDataToNeo4j
from modules.Neo4jDataAccess import Neo4jDataAccess

drugSynonym = IngestDrugSynonyms()
drugSynonym.auto_get_and_clean_data()

neo4jBridge = DrugSynonymDataToNeo4j(
    graph=Neo4jDataAccess().get_neo4j_graph(Neo4jDataAccess.RoleType.WRITER))
neo4jBridge.upload_drugs_and_synonyms(drugSynonym.drug_vocab)
neo4jBridge.upload_studies(drugSynonym.all_studies_df)
