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
            2: "Select count(denomination), code_ape.name from company, code_ape where code_ape.id = company.ID_CODEAPE group by code_ape.name",
            3: "Select denomination, forme_juridique.name as forme_juridique, code_ape.name as code_ape from company,forme_juridique,code_ape where company.id_formjur = forme_juridique.id AND company.id_codeape = code_ape.id",
            4: "Select denomination, addresse.name as adresse,code_postal,ville.name as ville from company, addresse, ville, region where company.id_addresse = addresse.id and addresse.id_ville = ville.id and ville.id_region = region.id",
            5: "Select forme_juridique.name as Forme_juridique, code_ape.name as Code_APE From forme_juridique, code_ape, company, addresse Where company.id_formjur = forme_juridique.id AND company.id_codeape = code_ape.id AND company.id_addresse = addresse.id AND addresse.code_postal = '77380'",
            6: "Select forme_juridique.name as Form_juridique, count(company.id) as n From company, addresse, ville, region,forme_juridique Where company.id_formjur = forme_juridique.id AND company.id_addresse = addresse.id AND addresse.id_ville = ville.id AND ville.id_region = region.id and region.id= '532' GROUP BY forme_juridique.name",
            7: "Select forme_juridique.name as Forme_juridique, code_ape.name as Code_APE, company.denomination as company From forme_juridique, code_ape, company, addresse, ville, region Where company.id_formjur = forme_juridique.id AND company.id_codeape = code_ape.id AND company.id_addresse = addresse.id AND addresse.id_ville = ville.id AND ville.id_region = region.id AND ville.id>='373' AND ville.id<='490' AND region.id>='319' AND region.id<='605'"
        }
        return switcher.get(number)

    def writeCsv(self,dbname,querynum,dataset,list):
        with open(dbname+"Results\\"+dbname+'Query'+querynum+'_'+dataset+'.csv', 'w') as csvfile:
            file_writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

            file_writer.writerow(list)