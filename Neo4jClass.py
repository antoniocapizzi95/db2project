from neo4j.v1 import GraphDatabase
from py2neo import Graph, Path


class Neo4jClass(object):

    def __init__(self, uri, user, password):
        self.graph = Graph(uri, auth=(user, password))

    def getRegion(self):
        ris = self.graph.run("MATCH (r:Region) RETURN r.name").data()
        l = []
        for elem in ris:
            e = elem['r.name']
            l.append(e)
        return l

    def getFormeJuridique(self):
        ris = self.graph.run("MATCH (f:FormeJuridique) RETURN f.name").data()
        l = []
        for elem in ris:
            e = elem['f.name']
            l.append(e)
        return l

    def getCodeApe(self):
        ris = self.graph.run("MATCH (c:CodeAPE) RETURN c.code,c.name").data()
        l = []
        for elem in ris:
            e = elem
            l.append(e)
        return l

    def getVille(self):
        ville = []

        ris = self.graph.run("MATCH p=(:Ville)-[r:HAS_REGION]->() RETURN p").data()
        for elem in ris:
            nodeville = elem['p'].nodes[0]
            nodereg = elem['p'].nodes[1]
            villename = nodeville['name']
            villename = villename.lower()
            #villeid = nodeville.identity
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
                ville.append({'ville': villename, 'region': regname})
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
                    ville.append({'ville': villename, 'region': regname})

        return ville

    def getAddresse(self): #to complete
        addresse = []
        ris = self.graph.run("MATCH p=(:Adresse)-[r:HAS_CODEPOSTAL]->() RETURN p").data()
        print("query completata")
        for elem in ris:
            nodeaddresse = elem['p'].nodes[0]
            nodecodepostal = elem['p'].nodes[1]
            addressename = nodeaddresse['name']
            addressename = addressename.lower()
            codepostalname = nodecodepostal['name']
            idcodepost = nodecodepostal.identity
            ris_ville = self.graph.run("MATCH p=(c:CodePostal)-[r:HAS_CITY]->() WHERE ID(c) ="+str(idcodepost)+" RETURN p").data()[0]
            nodeville = ris_ville['p'].nodes[1]
            ville = nodeville['name']
            ville = ville.lower()

            if len(addresse) == 0:
                addresse.append({'addresse': addressename, 'codepostal': codepostalname,'ville':ville})
            else:
                add = 0
                for e in addresse:
                    if addressename == e['addresse']:
                        add = 0
                        break
                    else:
                        add = 1
                if add == 1:
                    addresse.append({'addresse': addressename, 'codepostal': codepostalname, 'ville': ville})
        return addresse