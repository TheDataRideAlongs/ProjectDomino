from neo4j import GraphDatabase
from typing import Optional
from pandas import DataFrame
from numpy import isnan
import logging
from urllib.parse import urlparse

logger = logging.getLogger('ds-neo4j')

def dict_to_property_str(properties:Optional[dict] = None) -> str:

    def property_type_checker(property_value):
        if isinstance(property_value,int) or isinstance(property_value,float):
            pass
        elif isinstance(property_value,str):
            property_value = '''"''' + property_value.replace('"',r"\"") + '''"'''
        elif not property_value:
            property_value = "''"
        return property_value

    resp:str = ""
    if properties:
        resp = "{"
        for key in properties.keys():
            resp += """{key}:{value},""".format(key=key,value=property_type_checker(properties[key]))
        resp = resp[:-1] + "}"
    return resp

def cypher_template_filler(cypher_template:str,data:dict) -> str:
    return cypher_template.format(**data).replace("\n","")

class DrugSynonymDataToNeo4j(object):

    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="letmein", encrypted=False):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=encrypted)	        
        self.study_triald_and_neo4j_id_pairs:dict = {}
        self.drug_or_synonym_name_and_neo4j_id_pairs:dict = {}
        self.url_and_neo4j_id_pairs:dict = {}

    def reset_id_store(self):
        self.study_triald_and_neo4j_id_pairs = {}
        self.drug_or_synonym_name_and_neo4j_id_pairs = {}
        self.url_and_neo4j_id_pairs = {}
    
    def close(self):
        self._driver.close()
    
    @staticmethod
    def _merge_node(tx, node_type, properties:Optional[dict] = None):



        data:dict = {
            "node_type":node_type,
            "properties":dict_to_property_str(properties)
        }
        base_cypher = """
            MERGE (n:{node_type} {properties})
            RETURN id(n)
        """

        result = tx.run(cypher_template_filler(base_cypher,data))
        return result.single()[0]
    
    @staticmethod
    def _merge_edge(tx, from_id, to_id, edge_type, properties:Optional[dict] = None, direction = ">"):
        if not direction in [">",""]:
            raise ValueError

        data:dict = {
            "from_id":int(from_id),
            "to_id":int(to_id),
            "edge_type":edge_type,
            "direction":direction,
            "properties":dict_to_property_str(properties) 
        }
        base_cypher = """
            MATCH (from)
            WHERE ID(from) = {from_id}
            MATCH (to)
            WHERE ID(to) = {to_id}
            MERGE (from)-[r:{edge_type} {properties}]-{direction}(to)
            RETURN id(r)
        """
        result = tx.run(cypher_template_filler(base_cypher,data))
        return result.single()[0]

    def merge_studies(self,studies:DataFrame):
        node_merging_func = self._merge_node
        with self._driver.session() as session:
            logger.info("> Merging Studies Job is Started")
            count_node = 0
            prev_count_node = 0
            
            for study in studies.to_dict('records'):
                node_type = "Study"
                properties:dict = study
                study_id = session.write_transaction(node_merging_func, node_type, properties)
                self.study_triald_and_neo4j_id_pairs[study["trial_id"]] = study_id
                count_node += 1
                if count_node > prev_count_node + 1000:
                    prev_count_node = count_node
                    logger.info("> {} nodes already merged".format(count_node)) 

        logger.info("> Merging Studies Job is >> Done << with {} nodes merged".format(count_node)) 

    def merge_drugs_synonyms_and_link_between(self,drug_vocab):
        node_merging_func = self._merge_node
        edge_merging_func = self._merge_edge
        with self._driver.session() as session:
            logger.info("> Merging Drugs and Synonyms Job is Started to merge {} drugs with synonyms".format(len(drug_vocab)))
            count_node = 0
            count_edge = 0
            prev_count_node = 0
            prev_count_edge = 0

            for drug in drug_vocab.keys():
                node_type = "Drug"
                properties:dict = {
                    "name":drug
                }
                
                drug_id = session.write_transaction(node_merging_func, node_type, properties)
                self.drug_or_synonym_name_and_neo4j_id_pairs[drug] = drug_id
                count_node += 1
                if isinstance(drug_vocab[drug],list):
                    for synonym in drug_vocab[drug]:
                        node_type = "Synonym"
                        properties:dict = {
                            "name":synonym
                        }
                        synonym_id = session.write_transaction(node_merging_func, node_type, properties)
                        self.drug_or_synonym_name_and_neo4j_id_pairs[synonym] = synonym_id
                        count_node += 1

                        edge_type = "KNOWN_AS"
                        session.write_transaction(edge_merging_func, drug_id, synonym_id, edge_type)
                        count_edge += 1
                
                if count_node > prev_count_node + 1000 or  count_edge > prev_count_edge + 1000:
                    prev_count_node = count_node
                    prev_count_edge = count_edge
                    logger.info("> {} nodes and {} edges already merged".format(count_node,count_edge)) 
                    
            logger.info("> Merging Drugs and Synonyms Job is >> Done << with {} nodes and {} edges merged".format(count_node,count_edge))

    def merge_drug_to_study_rels(self,edges:list):
        edge_merging_func = self._merge_edge

        if self.study_triald_and_neo4j_id_pairs != {} and self.drug_or_synonym_name_and_neo4j_id_pairs != {}:
            study_id_lookup:dict = self.study_triald_and_neo4j_id_pairs
            drug_id_lookup:dict = self.drug_or_synonym_name_and_neo4j_id_pairs
            with self._driver.session() as session:
                logger.info("> Merging connections of Drugs&Synonyms to Studies Job is Started with {} edges to merge".format(len(edges)))
                edge_type = "APPEARED_IN"
                edge_ids:list = [session.write_transaction(edge_merging_func, drug_id_lookup[drug], study_id_lookup[trial_id], edge_type) for drug, trial_id in edges]
                logger.info("> Merging connections of Drugs&Synonyms to Studies Job is Finished with {} edges merged".format(len(edge_ids)))
        else:
            logger.warning("No Neo4j ID information is available for merging connections of Drugs&Synonyms to Studies")

    def merge_url(self,urls:list):
        node_merging_func = self._merge_node
        with self._driver.session() as session:
            logger.info("> Merging Urls Job is Started")
            count_node = 0
            prev_count_node = 0
            
            for url in urls:
                node_type = "Url"
                properties:dict = self._parse_url(url)
                url_id = session.write_transaction(node_merging_func, node_type, properties)
                self.url_and_neo4j_id_pairs[url] = url_id
                count_node += 1

                if count_node > prev_count_node + 1000:
                    prev_count_node = count_node
                    logger.info("> {} nodes already merged".format(count_node))

        logger.info("> Merging Url Job is >> Done << with {} nodes merged".format(count_node))

    def merge_url_to_study_rels(self,edges:list):
        edge_merging_func = self._merge_edge

        if self.study_triald_and_neo4j_id_pairs != {} and self.url_and_neo4j_id_pairs != {}:
            study_id_lookup:dict = self.study_triald_and_neo4j_id_pairs
            url_id_lookup:dict = self.url_and_neo4j_id_pairs
            with self._driver.session() as session:
                logger.info("> Merging connections of Urls to Studies Job is Started with {} edges to merge".format(len(edges)))
                edge_type = "POINTS_AT"
                edge_ids:list = [session.write_transaction(edge_merging_func, url_id_lookup[url], study_id_lookup[trial_id], edge_type) for url, trial_id in edges]
                logger.info("> Merging connections of Urls to Studies Job is Finished with {} edges merged".format(len(edge_ids)))
        else:
            logger.warning("No Neo4j ID information is available for Merging connections of Drugs&Synonyms to Studies")
    
    @staticmethod
    def _parse_url(url:str):
        parsed = urlparse(url)
        return {
                    'tweet_id': '',
                    'url': url,
                    'job_id': '',
                    'job_name': '',
                    'schema': parsed.scheme,
                    'netloc': parsed.netloc,
                    'path': parsed.path,
                    'params': parsed.params,
                    'query': parsed.query,
                    'fragment': parsed.fragment,
                    'username': parsed.username,
                    'password': parsed.password,
                    'hostname': parsed.hostname,
                    'port': parsed.port,
                }