from time import time
import csv
class QueryOracle(object) :
    def query1(self, orac):
        query = "select * from company"
        l = []
        for i in range(31):
            before = time()
            orac.connection.execute(query)
            after = time()
            result = (after - before) * 1000
            print(result)
            l.append(str(result))
        self.writeCsv("Oracle","1","100",l)

    def writeCsv(self,dbname,querynum,dataset,list):
        with open(dbname+"Results\\"+dbname+'Query'+querynum+'_'+dataset+'.csv', 'w') as csvfile:
            csvwriter = csv.writer(csvfile)
            for row in list:
                csvwriter.writerow(row)