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

    def getVille(self): #to complete
        ris = self.graph.run("MATCH (v:Ville) RETURN v.name").data()
        l = []
        for elem in ris:
            e = elem
            l.append(e)
        return l