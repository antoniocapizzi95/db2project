from time import time
import csv
class QueryNeo4j(object) :
    def query1(self,n4j,queryNum):
        query = self.chooseQuery(queryNum)
        l = []
        for i in range(31):
            before = time()
            n4j.graph.run(query)
            after = time()
            result = (after - before) * 1000
            print(result)
            l.append(str(result))
        self.writeCsv("Neo4j",str(queryNum),"100",l)


    def chooseQuery(self,number):
        switcher = {
            1: "MATCH (n:Company) where n.DateImmatriculation>'2014-10-01' and n.DateImmatriculation>'2015-04-01' RETURN n",
            2: "MATCH p=(c:Company)-[r:HAS_CODEAPE]->(a:CodeAPE) RETURN a.name AS CodeAPEName, count(c.Denomination) AS Company",
            3: "MATCH p=(c:Company)-[r:HAS_FORMEJURIDIQUE]->(), s=(c:Company)-[f:HAS_CODEAPE]->() RETURN p,s",
            4: "MATCH p=(c:Company)-[r:HAS_ADRESSE]->(a:Adresse),g=(a)-[:HAS_CODEPOSTAL]->(co:CodePostal),f=(co)-[:HAS_CITY]->() RETURN p,g,f",
            5: "May",
            6: "June",
            7: "July"
        }
        return switcher.get(number)

    def writeCsv(self,dbname,querynum,dataset,list):
        with open(dbname+"Results\\"+dbname+'Query'+querynum+'_'+dataset+'.csv', 'w') as csvfile:
            file_writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

            file_writer.writerow(list)