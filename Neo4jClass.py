from neo4j.v1 import GraphDatabase,basic_auth
from py2neo import Graph, Path
import csv


class Neo4jClass(object):

    def __init__(self, uri, user, password):
        self.graph = Graph(uri, auth=(user, password))

    def testQuery(self):
        uri = "bolt://localhost:7687"
        auth_token = basic_auth("neo4j", "prova")
        driver = GraphDatabase.driver(uri, auth=auth_token)

        session = driver.session()

        q = "MATCH (n:Company) RETURN n"
        l = []
        for i in range(30):
            result = session.run(q)
            #print(result.result_available_after)
            avail = result.summary().result_available_after
            cons = result.summary().result_consumed_after
            total_time = avail + cons
            l.append(str(total_time))
            print(str(total_time) + " ms")
        with open('output.csv', 'w') as csvfile:
            csvwriter = csv.writer(csvfile)
            for row in l:
                csvwriter.writerow(row)

    def getRegion(self):
        ris = self.graph.run("MATCH (r:Region) RETURN r").data()
        l = []
        for elem in ris:
            node = elem['r']
            name = node['name']
            id = node.identity
            l.append({'id':id,'name':name})
        return l

    def getFormeJuridique(self):
        ris = self.graph.run("MATCH (f:FormeJuridique) RETURN f").data()
        l = []
        for elem in ris:
            node = elem['f']
            name = node['name']
            id = node.identity
            l.append({'id':id,'name':name})
        return l

    def getCodeApe(self):
        ris = self.graph.run("MATCH (c:CodeAPE) RETURN c").data()
        l = []
        for elem in ris:
            node = elem['c']
            name = node['name']
            code = node['code']
            id = node.identity
            l.append({'id':id,'code':code,'name':name})
        return l

    def getVille(self):
        ville = []

        ris = self.graph.run("MATCH p=(:Ville)-[r:HAS_REGION]->() RETURN p").data()
        for elem in ris:
            nodeville = elem['p'].nodes[0]
            nodereg = elem['p'].nodes[1]
            villename = nodeville['name']
            villename = villename.lower()
            villeid = nodeville.identity
            regname = nodereg['name']

            #riscodepostal = []
            #codepostal = ''
            #try:
             #   riscodepostal = self.graph.run("MATCH p=()-[r:HAS_CITY]->(v:Ville) where ID(v) = "+str(villeid)+" RETURN p").data()[0]
             #  nodecodepostal = riscodepostal['p'].nodes[0]
             #  codepostal = nodecodepostal['name']
            #except:
             #   codepostal = '0'


            if len(ville) == 0:
                #ville.append({'ville': villename, 'region': regname, 'codepostal': codepostal})
                ville.append({'id':villeid,'ville': villename, 'region': regname})
            else:
                add = 0
                for e in ville:
                    if villename == e['ville']:
                        add = 0
                        break
                    else:
                        add = 1
                if add == 1:
                    #ville.append({'ville': villename, 'region': regname, 'codepostal': codepostal})
                    ville.append({'id':villeid,'ville': villename, 'region': regname})

        return ville

    def getAddresse(self):
        addresse = []
        ris = self.graph.run("MATCH p=(:Adresse)-[r:HAS_CODEPOSTAL]->() RETURN p").data()
        for elem in ris:
            nodeaddresse = elem['p'].nodes[0]
            nodecodepostal = elem['p'].nodes[1]
            addressename = nodeaddresse['name']
            addressename = addressename.lower()
            codepostalname = nodecodepostal['name']
            idaddresse = nodeaddresse.identity
            idcodepost = nodecodepostal.identity
            ris_ville = self.graph.run("MATCH p=(c:CodePostal)-[r:HAS_CITY]->() WHERE ID(c) ="+str(idcodepost)+" RETURN p").data()[0]
            nodeville = ris_ville['p'].nodes[1]
            ville = nodeville['name']
            ville = ville.lower()

            if len(addresse) == 0:
                addresse.append({'id':idaddresse,'addresse': addressename, 'codepostal': codepostalname,'ville':ville})
            else:
                add = 0
                for e in addresse:
                    if addressename == e['addresse']:
                        add = 0
                        break
                    else:
                        add = 1
                if add == 1:
                    addresse.append({'id': idaddresse, 'addresse': addressename, 'codepostal': codepostalname, 'ville': ville})
        return addresse

    def getCompany(self,oracle):
        ris = self.graph.run("MATCH (n:Company) RETURN n").data()
        for elem in ris:
            nodecompany = elem['n'].nodes[0]
            denomination = nodecompany['Denomination']
            geoloc = nodecompany['Geolocalisation']
            fiche_ent = nodecompany['FicheEntreprise']
            date_imm = nodecompany['DateImmatriculation']
            greffe = nodecompany['Greffe']
            code_siren = nodecompany['CodeSiren']
            id = nodecompany.identity
            id_formejur = self.getIDFormeJurForCompany(id,oracle)
            id_codeape = self.getIDCodeAPEForCompany(id,oracle)
            id_adresse = self.getIDAddresseForCompany(id,oracle)
            if id_codeape == None:
                id_codeape='NULL'
            if fiche_ent == None:
                fiche_ent = 'NULL'
            if geoloc == None:
                geoloc = 'NULL'
            if code_siren == None:
                code_siren = 'NULL'
            #geoloc = geoloc.replace(","," -")
            denomination = denomination.replace("'"," ")
            index = str(id)
            try:
                values = "("+"'"+index+"',"+ "'"+denomination+"',"+"'"+code_siren+"',"+"'"+date_imm+"',"+"'"+fiche_ent+"',"+"'"+geoloc+"',"+"'"+greffe+"',"+"'"+id_adresse+"',"+"'"+id_formejur+"',"+"'"+id_codeape+"'"+ ")"
            except:
                print("errore")
            ins = "insert into company (id, denomination, code_siren, date_immatriculation, fiche_enterprise, geolocalisation, greffe, id_addresse, id_formjur, id_codeape) values "+values
            ins = ins.encode('ascii', 'ignore').decode('ascii')
            try:
                oracle.connection.execute(ins)
            except:
                print("Errore: "+ins)
        oracle.conn.commit()





    def getIDFormeJurForCompany(self,id,oracle):
        id_formejur = ''
        try:
            ris_formejur = self.graph.run("MATCH p=(c:Company)-[r:HAS_FORMEJURIDIQUE]->() WHERE ID(c) = " + str(id) + " RETURN p").data()[0]
            node_formejur = ris_formejur['p'].nodes[1]
            name = node_formejur['name']
            name = name.replace("'", " ")
            query = "select id from forme_juridique where name = " + "'" + name + "'"
            query = query.encode('ascii', 'ignore').decode('ascii')
            oracle.connection.execute(query)
            for row in oracle.connection:
                id_formejur = row[0]
                id_formejur = str(id_formejur)
                break
            return id_formejur
        except:
            return None




    def getIDCodeAPEForCompany(self,id,oracle):
        try:
            ris_codeape = self.graph.run("MATCH p=(c:Company)-[r:HAS_CODEAPE]->() WHERE ID(c) = " + str(id) + " RETURN p").data()[0]
            node_codeape = ris_codeape['p'].nodes[1]
            code = node_codeape['code']
            code = code.replace("'", " ")
        except:
            code = None
        if code != None:
            query = "select id from code_ape where code = " + "'" + code + "'"
            query = query.encode('ascii', 'ignore').decode('ascii')
            oracle.connection.execute(query)
            id_code = ''
            for row in oracle.connection:
                id_code = row[0]
                id_code = str(id_code)
                break
            return id_code
        else:
            return code

    def getIDAddresseForCompany(self,id,oracle):
        try:
            ris_adresse = self.graph.run("MATCH p=(c:Company)-[r:HAS_ADRESSE]->() WHERE ID(c) = " + str(id) + " RETURN p").data()[0]
            node_adresse = ris_adresse['p'].nodes[1]
            id = node_adresse.identity
            query = "select id from addresse where id = " + "'" + str(id) + "'"
            query = query.encode('ascii', 'ignore').decode('ascii')
            oracle.connection.execute(query)
            for row in oracle.connection:
                id_adresse = row[0]
                id_adresse = str(id_adresse)
                break
            return id_adresse
        except:
            return ''
