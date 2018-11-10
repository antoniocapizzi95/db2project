from neo4j.v1 import GraphDatabase, basic_auth

from time import time
import csv
class QueryNeo4j(object) :

    def connect(self):
        driver = GraphDatabase.driver("bolt://localhost:7687", auth = basic_auth("neo4j", "prova"))
        self.session = driver.session()


    def query(self,queryNum):
        query = self.chooseQuery(queryNum)
        l = []
        for i in range(31):
            self.connect()
            before = time()
            #n4j.graph.run(query)
            cur = self.session.run(query)
            for c in cur:
                    break
            after = time()
            result = (after - before) * 1000
            print(str(result) + " neo4j query" + str(queryNum))
            l.append(str(result))
            #self.session.close()
        self.writeCsv("Neo4j",str(queryNum),"100",l)


    def chooseQuery(self,number):
        switcher = {
            1: "MATCH (n:Company) where n.DateImmatriculation>'2014-10-01' and n.DateImmatriculation<'2015-04-01' RETURN n",
            2: "MATCH p=(c:Company)-[r:HAS_CODEAPE]->(a:CodeAPE) RETURN a.name AS CodeAPEName, count(c.Denomination) AS Company",
            3: "MATCH p=(c:Company)-[r:HAS_FORMEJURIDIQUE]->(fj:FormeJuridique), s=(c)-[f:HAS_CODEAPE]->(co:CodeAPE) RETURN c.Denomination,fj.name,co.name",
            4: "MATCH p=(c:Company)-[r:HAS_ADRESSE]->(a:Adresse),g=(a)-[:HAS_CODEPOSTAL]->(co:CodePostal),f=(co)-[:HAS_CITY]->(vi:Ville) RETURN c.Denomination AS Company,a.name AS Adresse,co.name AS CodePostal,vi.name AS Ville",
            5: "MATCH f=(c:Company)-[r:HAS_FORMEJURIDIQUE]->(fo:FormeJuridique),co=(c)-[:HAS_CODEAPE]->(ape:CodeAPE), ad=(c)-[:HAS_ADRESSE]->(a:Adresse), cp=(a)-[:HAS_CODEPOSTAL]->(cod:CodePostal) WHERE cod.name='77380' RETURN fo as Forme_juridique, ape as CodeAPE",
            6: "MATCH p=(c:Company)-[r:HAS_FORMEJURIDIQUE]->(fj:FormeJuridique),a=(c)-[:HAS_ADRESSE]->(add:Adresse),s=(add)-[:HAS_CODEPOSTAL]->(co:CodePostal),f=(co)-[:HAS_CITY]->(ci:Ville),g=(ci)-[:HAS_REGION]->(re:Region) WHERE ID(re)=532 RETURN fj,count(c)",
            7: "MATCH p=(c:Company)-[r:HAS_FORMEJURIDIQUE]->(fj:FormeJuridique), ca=(c)-[:HAS_CODEAPE]->(coa:CodeAPE),a=(c)-[:HAS_ADRESSE]->(add:Adresse),s=(add)-[:HAS_CODEPOSTAL]->(co:CodePostal),f=(co)-[:HAS_CITY]->(ci:Ville),g=(ci)-[:HAS_REGION]->(re:Region) WHERE ID(re)>=319 AND ID(re)<=585 RETURN fj.name as Forme_juridique,coa.name as CodeAPE,c.Denomination as Company"
        }
        return switcher.get(number)

    def writeCsv(self,dbname,querynum,dataset,list):
        with open(dbname+"Results\\"+dbname+'Query'+querynum+'_'+dataset+'.csv', 'w') as csvfile:
            file_writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

            file_writer.writerow(list)