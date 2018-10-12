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

        ris = self.graph.run("MATCH p=(:Ville)-[r:HAS_REGION]->() RETURN p LIMIT 5").data()
        for elem in ris:
            nodeville = elem['p'].nodes[0]
            nodereg = elem['p'].nodes[1]
            villename = nodeville['name']
            villeid = nodeville.identity
            regname = nodereg['name']

            riscodepostal = self.graph.run("MATCH p=()-[r:HAS_CITY]->(v:Ville) where ID(v) = "+str(villeid)+" RETURN p").data()[0]
            nodecodepostal = riscodepostal['p'].nodes[0]
            codepostal = nodecodepostal['name']
            if len(ville) == 0:
                ville.append({'ville': villename, 'region': regname, 'codepostal': codepostal})
            else:
                add = 0
                for e in ville:
                    if villename in e['ville']:
                        add = 0
                        break
                    else:
                        add = 1
                if add == 1:
                    ville.append({'ville': villename, 'region': regname, 'codepostal': codepostal})

        return ville

