from time import time
import csv
class QueryNeo4j(object) :
    def query1(self,n4j):
        query = "MATCH (n:Company) RETURN n"
        l = []
        for i in range(31):
            before = time()
            n4j.graph.run(query)
            after = time()
            result = (after - before) * 1000
            print(result)
            l.append(str(result))
        self.writeCsv("Neo4j","1","100",l)

    def writeCsv(self,dbname,querynum,dataset,list):
        with open(dbname+"Results\\"+dbname+'Query'+querynum+'_'+dataset+'.csv', 'w') as csvfile:
            csvwriter = csv.writer(csvfile)
            for row in list:
                csvwriter.writerow(row)