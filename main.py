import OracleClass
import Neo4jClass
import QueryOracle
import QueryNeo4j
#n = Neo4jClass.Neo4jClass("bolt://localhost:7687","neo4j","prova")
o = OracleClass.OracleClass("antonio","prova")
qn = QueryNeo4j.QueryNeo4j()
qo = QueryOracle.QueryOracle()
#o.insertRegion(n)
#o.insertFormeJuridique(n)
#o.insertCodeAPE(n)
#o.insertVille(n)
#o.insertAddresse(n)
#n.getAddresse2(o)
#n.getCompany(o)
for i in range(1,8):
    #qn.query(i)
    qo.query(o,i)
#o.showTable("addresse")
#print("finito")
o.closeConnection()


