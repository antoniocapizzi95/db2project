from time import time
import csv
class QueryOracle(object) :
    def query(self, orac, queryNum):
        query = self.chooseQuery(queryNum)
        l = []
        for i in range(31):
            before = time()
            orac.connection.execute(query)
            after = time()
            result = (after - before) * 1000
            print(result)
            l.append(str(result))
        self.writeCsv("Oracle",str(queryNum),"100",l)


    def chooseQuery(self,number):
        switcher = {
            1: "Select * From Company WHERE date_immatriculation > '2014-10-01' AND date_immatriculation < '2015-04-01'",
            2: "select denomination, ville.name as ville from company, addresse,ville where company.id_addresse = addresse.id and addresse.id_ville = ville.id",
            3: "Select denomination, forme_juridique.name as forme_juridique, code_ape.name as code_ape from company,forme_juridique,code_ape where company.id_formjur = forme_juridique.id AND company.id_codeape = code_ape.id",
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