from neo4j import GraphDatabase
from typing import Optional
import logging
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger('ds-neo4j')

def dict_to_property_str(properties:Optional[dict] = None) -> str:

    def property_type_checker(property_value):
        if isinstance(property_value,int) or isinstance(property_value,float):
            pass
        elif isinstance(property_value,str):
            property_value = '''"''' + property_value + '''"'''
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

    def close(self):
        self._driver.close()
    
    def upload_drugs_and_synonyms(self,drug_vocab):
        node_merging_func = self._merge_node
        edge_merging_func = self._merge_edge
        with self._driver.session() as session:
            logger.info("> Importing Drugs and Synonyms Job is Started")
            count_node = 0
            count_edge = 0
            prev_count_node = 0
            prev_count_edge = 0

            for key in drug_vocab.keys():
                node_type = "Drug"
                properties:dict = {
                    "name":key
                }
                
                drug_id = session.write_transaction(node_merging_func, node_type, properties)
                count_node += 1
                if isinstance(drug_vocab[key],list):
                    for synonym in drug_vocab[key]:
                        node_type = "Synonym"
                        properties:dict = {
                            "name":synonym
                        }
                        synonym_id = session.write_transaction(node_merging_func, node_type, properties)
                        count_node += 1

                        edge_type = "KNOWN_AS"
                        session.write_transaction(edge_merging_func, drug_id, synonym_id, edge_type)
                        count_edge += 1
                
                if count_node > prev_count_node + 1000 or  count_edge > prev_count_edge + 1000:
                    prev_count_node = count_node
                    prev_count_edge = count_edge
                    logger.info("> {} nodes and {} edges already imported".format(count_node,count_edge)) 
                    
            logger.info("> Importing Drugs and Synonyms Job is >> Done << with {} nodes and {} edges imported".format(count_node,count_edge)) 
    
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

