import OracleClass
import Neo4jClass
n = Neo4jClass.Neo4jClass("bolt://localhost:7687","neo4j","prova")
o = OracleClass.OracleClass("antonio","prova")
#o.insertRegion(n)
#o.insertFormeJuridique(n)
#o.insertCodeAPE(n)
o.insertVille(n)
o.showTable("ville")
o.closeConnection()


