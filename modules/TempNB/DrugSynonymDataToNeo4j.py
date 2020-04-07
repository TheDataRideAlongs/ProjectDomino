from neo4j import GraphDatabase
from typing import Optional

class DrugSynonymDataToNeo4j(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()
    
    def upload_drugs_and_synonims(self,drug_vocab):
        with self._driver.session() as session:
            for key in drub_vocab.keys():
                node_type = "Drug"
                properties:dict = {
                    "name":key
                }
                drug_id = session.write_transaction(self._merge_node, node_type, properties)
                for synonym in drug_vocab[key]:
                    node_type = "Synonym"
                    properties:dict = {
                        "name":synonym
                    }
                    synonym_id = session.write_transaction(self._merge_node, node_type, properties)
                    edge_type = "HAS"
                    session.write_transaction(self._merge_edge, drug_id, synonym_id, edge_type)
            print("Done") 
    
    @staticmethod
    def _merge_node(tx, node_type, properties=None):
        data:dict = {
            "node_type":node_type,
            "properties":self._dict_to_property_str(properties)
        }
        # '{first} {last}'.format(**data)
        base_cypher = """
        MERGE (n:{node_type}) {{ {properties} }})
        RETURN id(n)
        """
        result = tx.run(base_cypher.format(**data))
        
        return result
    
    @staticmethod
    def _merge_edge(tx, from_id, to_id, edge_type, direction = ">"):
        if not direction in [">",""]:
            raise ValueError
        data:dict = {
            "from_id":int(from_id),
            "to_id":int(to_id),
            "edge_type":edge_type,
            "directon":direction 
        }
        base_cypher = """    
        MERGE (Node({from_id}))-[r:{edge_type}]-{direction}(Node({to_id}))
        RETURN id(r)
        """
        result = tx.run(base_cypher.format(**data))
        return result
    
    @staticmethod
    def _dict_to_property_str(properties:Optional[dict] = None) -> str:
        def property_type_checker(property_value):
            if isinstance(property_value,int) or isinstance(property_value,float):
                pass
            elif isinstance(property_value,str):
                property_value = """'""" + property_value + """'"""
            return property_value

        resp = ""
        if not properties:
            resp = "{"
            for key in properties.keys():
                resp += """{key}:{value},""".format(key=key,value=property_type_checker(properties[key]))
            resp = resp[:-1] + "}"
        return resp

