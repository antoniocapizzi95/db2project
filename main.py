import OracleClass
import Neo4jClass
n = Neo4jClass.Neo4jClass("bolt://localhost:7687","neo4j","prova")
n.print_greeting("prova")
o = OracleClass.OracleClass()
o.connect()


