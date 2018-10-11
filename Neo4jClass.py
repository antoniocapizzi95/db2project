from neo4j.v1 import GraphDatabase
from py2neo import Graph, Path

class Neo4jClass(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def print_greeting(self, message):
        with self._driver.session() as session:
            greeting = session.write_transaction(self._create_and_return_greeting, message)
            print(greeting)

    @staticmethod
    def _create_and_return_greeting(tx, message):
        result = tx.run("CREATE (a:Greeting) "
                        "SET a.message = $message "
                        "RETURN a.message + ', from node ' + id(a)", message=message)
        return result.single()[0]

    def trypy2neo(self):
        graph = Graph("bolt://localhost:7687",auth= ('neo4j', 'prova'))
        ris = graph.run("MATCH (r:Region) RETURN r LIMIT 4").data()
        for elem in ris:
            print(elem)