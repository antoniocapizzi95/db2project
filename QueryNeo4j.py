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
            1: "",
            2: "",
            3: "",
            4: "April",
            5: "May",
            6: "June",
            7: "July"
        }
        return switcher.get(number)

    def writeCsv(self,dbname,querynum,dataset,list):
        with open(dbname+"Results\\"+dbname+'Query'+querynum+'_'+dataset+'.csv', 'w') as csvfile:
            file_writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

            file_writer.writerow(list)