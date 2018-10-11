import OracleClass
import Neo4jClass
n = Neo4jClass.Neo4jClass("bolt://localhost:7687","neo4j","prova")
o = OracleClass.OracleClass("antonio","prova")
#o.insertRegion(n)
#o.showTable("region")
#o.insertFormeJuridique(n)
#o.showTable("forme_juridique")
#o.insertCodeAPE(n)
o.showTable("region")


