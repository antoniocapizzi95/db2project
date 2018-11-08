from time import time
import csv
class QueryNeo4j(object) :
    def query(self,n4j,queryNum):
        query = self.chooseQuery(queryNum)
        l = []
        for i in range(31):
            before = time()
            n4j.graph.run(query)
            after = time()
            result = (after - before) * 1000
            print(str(result) + " neo4j query" + str(queryNum))
            l.append(str(result))
        self.writeCsv("Neo4j",str(queryNum),"100",l)


    def chooseQuery(self,number):
        switcher = {
            1: "MATCH (n:Company) where n.DateImmatriculation>'2014-10-01' and n.DateImmatriculation<'2015-04-01' RETURN n",
            2: "MATCH p=(c:Company)-[r:HAS_CODEAPE]->(a:CodeAPE) RETURN a.name AS CodeAPEName, count(c.Denomination) AS Company",
            3: "MATCH p=(c:Company)-[r:HAS_FORMEJURIDIQUE]->(), s=(c:Company)-[f:HAS_CODEAPE]->() RETURN p,s",
            4: "MATCH p=(c:Company)-[r:HAS_ADRESSE]->(a:Adresse),g=(a)-[:HAS_CODEPOSTAL]->(co:CodePostal),f=(co)-[:HAS_CITY]->() RETURN p,g,f",
            5: "MATCH f=(c:Company)-[r:HAS_FORMEJURIDIQUE]->(fo:FormeJuridique),co=(c)-[:HAS_CODEAPE]->(ape:CodeAPE), ad=(c)-[:HAS_ADRESSE]->(a:Adresse), cp=(a)-[:HAS_CODEPOSTAL]->(cod:CodePostal) WHERE cod.name='77380' RETURN fo as Forme_juridique, ape as CodeAPE",
            6: "MATCH p=(c:Company)-[r:HAS_FORMEJURIDIQUE]->(fj:FormeJuridique),a=(c)-[:HAS_ADRESSE]->(add:Adresse),s=(add)-[:HAS_CODEPOSTAL]->(co:CodePostal),f=(co)-[:HAS_CITY]->(ci:Ville),g=(ci)-[:HAS_REGION]->(re:Region) WHERE ID(re)=236239 RETURN fj,count(c)",
            7: "MATCH p=(c:Company)-[r:HAS_FORMEJURIDIQUE]->(fj:FormeJuridique), ca=(c)-[:HAS_CODEAPE]->(coa:CodeAPE),a=(c)-[:HAS_ADRESSE]->(add:Adresse),s=(add)-[:HAS_CODEPOSTAL]->(co:CodePostal),f=(co)-[:HAS_CITY]->(ci:Ville),g=(ci)-[:HAS_REGION]->(re:Region) WHERE ID(ci)>=2619 AND ID(ci)<=3977 AND ID(re)>=3943 AND ID(re)<=4877 RETURN fj as Forme_juridique,coa as CodeAPE,c as Company"
        }
        return switcher.get(number)

    def writeCsv(self,dbname,querynum,dataset,list):
        with open(dbname+"Results\\"+dbname+'Query'+querynum+'_'+dataset+'.csv', 'w') as csvfile:
            file_writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

            file_writer.writerow(list)