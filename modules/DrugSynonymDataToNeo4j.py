from neo4j import GraphDatabase
from typing import Optional
from pandas import DataFrame
from numpy import isnan
import logging
from urllib.parse import urlparse
from progress.bar import Bar

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

def generate_unwind_property_cypher(properties:list,unwind_iterator_item_name:str) -> str:
    resp:str = ""
    if properties != [] and properties[0] != {}:
        resp = "{"
        for key in properties[0].keys():
            resp += """{key}:{unwind_iterator_item_name}.{key},""".format(key=key,unwind_iterator_item_name=unwind_iterator_item_name)
        resp = resp[:-1] + "}"
    return resp

class DrugSynonymDataToNeo4j(object):

    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="letmein", encrypted=False):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=encrypted)	        
        self.id_store:dict = {}

    def reset_id_store(self):
        self.id_store:dict = {}
    
    def close(self):
        self._driver.close()
   
    def batch_node_merge_handler(self,raw_data,generate_nodes_list,generate_node_data, node_type:str, chunk_size = 1000):
        logger.info("Merging '{}' Job is Started to merge {} nodes".format(node_type,len(raw_data)))
        node_merging_func = self._batch_merge_nodes
        
        node_ids:list = []

        nodes_list:list = generate_nodes_list(raw_data)

        nodes_data = generate_node_data(raw_data)
        properties = generate_unwind_property_cypher(nodes_data,unwind_iterator_item_name = "node")

        with self._driver.session() as session:
        
            with Bar("Loading '{}' nodes".format(node_type), fill='@', suffix='%(percent)d%%',max=len(nodes_list)) as bar:
                for i in range(0, len(nodes_data), chunk_size):
                    nodes_data_slice = nodes_data[i:i + chunk_size]

                    node_ids.extend(session.write_transaction(node_merging_func, node_type, nodes_data_slice, properties))
                    bar.next(chunk_size)
        
        self.id_store.update({key:value for key,value in zip(nodes_list,node_ids)})
        
        logger.info("Merging '{}' Job is >> Done << to merge {} nodes".format(node_type,len(node_ids)))
    
    @staticmethod
    def generate_drug_and_synonym_edge_props(raw_data:list) -> list:
        return [prop for fro,to,prop in raw_data]

    @staticmethod
    def generate_drug_and_synonym_edge_list_data(raw_data:list,id_store:dict) -> list:
        return [dict({"from_id":id_store[fro],"to_id":id_store[to]},**prop) for fro,to,prop in raw_data]

    def batch_edge_merge_handler(self, raw_data, generate_edge_data, generate_edge_props, edge_type:str, chunk_size = 1000):
        logger.info("Merging '{}' Job is Started to merge {} edges".format(edge_type,len(raw_data)))
        edge_merging_func = self._batch_merge_edges
        
        edges_data = generate_edge_data(raw_data, self.id_store)
        properties = generate_unwind_property_cypher(generate_edge_props(raw_data),unwind_iterator_item_name = "edge")

        with self._driver.session() as session:
        
            with Bar("Loading '{}' edges".format(edge_type), fill='@', suffix='%(percent)d%%',max=len(edges_data)) as bar:
                for i in range(0, len(edges_data), chunk_size):
                    edges_data_slice = edges_data[i:i + chunk_size]

                    session.write_transaction(edge_merging_func, edge_type, edges_data_slice, properties)
                    bar.next(chunk_size)
        
        logger.info("Merging '{}' Job is >> Done << to merge {} edges".format(edge_type,len(edges_data)))
    
    def merge_drug_to_synonym_rels(self,drug_synonym_rels):
        self.batch_edge_merge_handler(drug_synonym_rels,self.generate_drug_and_synonym_edge_list_data,self.generate_drug_and_synonym_edge_props,edge_type="KNOWN_AS")

    @staticmethod
    def generate_drug_nodes_list(drugs:list) -> list:
        return drugs

    @staticmethod
    def generate_drug_node_data(drugs:list) -> list:
        return [{"name":drug} for drug in drugs]

    def merge_drugs(self,drug_vocab):
        self.batch_node_merge_handler(drug_vocab,self.generate_drug_nodes_list,self.generate_drug_node_data,node_type="Drug")

    @staticmethod
    def generate_synonym_nodes_list(synonyms:list) -> list:
        return synonyms

    @staticmethod
    def generate_synonym_node_data(synonyms:list) -> list:
        return [{"name":synonym} for synonym in synonyms]

    def merge_synonyms(self,drug_vocab):
        self.batch_node_merge_handler(drug_vocab,self.generate_synonym_nodes_list,self.generate_synonym_node_data,node_type="Synonym")
        
    @staticmethod
    def _batch_merge_nodes(tx, node_type, nodes_data_slice:dict, properties:str):
        data:dict = {
            "node_type":node_type,
            "properties":properties
        }
        base_cypher = """
            UNWIND $nodes as node
            MERGE (n:{node_type} {properties})
            RETURN id(n) as id
        """

        result = tx.run(cypher_template_filler(base_cypher,data),nodes=nodes_data_slice)
        return [int(item["id"]) for item in result]
    
    @staticmethod
    def _batch_merge_edges(tx, edge_type, edges_data_slice:dict, properties:str, direction = ">"):
        data:dict = {
            "edge_type":edge_type,
            "properties":properties,
            "direction":direction
        }
        base_cypher = """
            UNWIND $edges as edge
            MATCH (from)
            WHERE ID(from) = edge.from_id
            MATCH (to)
            WHERE ID(to) = edge.to_id
            MERGE (from)-[r:{edge_type} {properties}]-{direction}(to)
            RETURN id(r) as id
        """

        result = tx.run(cypher_template_filler(base_cypher,data),edges=edges_data_slice)
        return [int(item["id"]) for item in result]

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
    
    @staticmethod
    def generate_study_nodes_list(studies:DataFrame) -> list:
        return studies["trial_id"]

    @staticmethod
    def generate_study_node_data(studies:DataFrame) -> list:
        return studies.to_dict('records')

    def merge_studies(self,studies:DataFrame):
        self.batch_node_merge_handler(studies,self.generate_study_nodes_list,self.generate_study_node_data,node_type="Study")

    @staticmethod
    def generate_drug_to_study_edge_props(raw_data:list) -> list:
        return [prop for fro,to,prop in raw_data]

    @staticmethod
    def generate_drug_to_study_list_data(raw_data:list,id_store:dict) -> list:
        return [dict({"from_id":id_store[fro],"to_id":id_store[to]},**prop) for fro,to,prop in raw_data]

    def merge_drug_to_study_rels(self,edges:list):
        self.batch_edge_merge_handler(edges,self.generate_drug_to_study_list_data,self.generate_drug_to_study_edge_props,edge_type="APPEARED_IN")

    @staticmethod
    def generate_url_nodes_list(urls:list) -> list:
        return urls

    def generate_url_node_data(self,urls:list) -> list:
        return [self._parse_url(url) for url in urls]


    def merge_url(self,urls:list):
        self.batch_node_merge_handler(urls,self.generate_url_nodes_list,self.generate_url_node_data,node_type="Url")

    @staticmethod
    def generate_url_to_study_edge_props(raw_data:list) -> list:
        return [prop for fro,to,prop in raw_data]

    @staticmethod
    def generate_url_to_study_list_data(raw_data:list,id_store:dict) -> list:
        return [dict({"from_id":id_store[fro],"to_id":id_store[to]},**prop) for fro,to,prop in raw_data]

    def merge_url_to_study_rels(self,edges:list):
        self.batch_edge_merge_handler(edges,self.generate_url_to_study_list_data,self.generate_url_to_study_edge_props,edge_type="POINTS_AT")

    @staticmethod
    def _parse_url(url:str):
        parsed = urlparse(url)
        return {
                    'tweet_id': '',
                    'url': url,
                    'job_id': '',
                    'job_name': '',
                    'schema': parsed.scheme if parsed.scheme else '',
                    'netloc': parsed.netloc if parsed.netloc else '',
                    'path': parsed.path if parsed.path else '',
                    'params': parsed.params if parsed.params else '',
                    'query': parsed.query if parsed.query else '',
                    'fragment': parsed.fragment if parsed.fragment else '',
                    'username': parsed.username if parsed.username else '',
                    'password': parsed.password if parsed.password else '',
                    'hostname': parsed.hostname if parsed.hostname else '',
                    'port': parsed.port if parsed.port else '',
                }
    




