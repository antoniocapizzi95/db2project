import cx_Oracle



class OracleClass(object) :
    def __init__(self,user,password):

        dsn_tns = cx_Oracle.makedsn('localhost', '1521',
                                    service_name='orcl')  # if needed, place an 'r' before any parameter in order to address any special character such as '\'.
        self.conn = cx_Oracle.connect(user=user, password=password,
                                 dsn=dsn_tns)  # if needed, place an 'r' before any parameter in order to address any special character such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
        self.connection = self.conn.cursor()


    def closeConnection(self):
        self.connection.close()

    def insertRegion(self,n4j):
        region = n4j.getRegion()
        i = 0
        #ins = "insert into region (id,name) values (0,'test')"
        #self.connection.execute(ins)
        for elem in region:
            elemMod = elem.replace("'"," ")
            index = str(i)
            ins = "insert into region (id,name) values ("+index+","+"'"+elemMod+"'"+")"
            ins = ins.encode('ascii', 'ignore').decode('ascii')
            self.connection.execute(ins)
            i = i + 1
        self.conn.commit()

    def insertFormeJuridique(self,n4j):
        formejur = n4j.getFormeJuridique()
        i = 0
        for elem in formejur:
            elemMod = elem.replace("'"," ")
            index = str(i)
            ins = "insert into forme_juridique (id,name) values ("+index+","+"'"+elemMod+"'"+")"
            ins = ins.encode('ascii', 'ignore').decode('ascii')
            self.connection.execute(ins)
            i = i + 1
        self.conn.commit()

    def insertCodeAPE(self, n4j):
        codeape = n4j.getCodeApe()
        i = 0
        for elem in codeape:
            code = elem['c.code']
            name = elem['c.name'].replace("'", " ")
            index = str(i)
            ins = "insert into code_ape (id,code,name) values (" + index + "," + "'" + code + "'" +","+ "'" +name+ "'" +")"
            ins = ins.encode('ascii', 'ignore').decode('ascii')
            self.connection.execute(ins)
            i = i + 1
        self.conn.commit()

    def insertVille(self, n4j): #to complete
        ville = n4j.getVille()
        i = 0
        for elem in ville:
            regname = elem['region']
            regname = regname.replace("'"," ")
            regname = regname.encode('ascii', 'ignore').decode('ascii')
            idreg = ''
            self.connection.execute('select id from region where name='+"'"+regname+"'")
            for row in self.connection:
                idreg = row[0]
                break
            index = str(i)
            idreg = str(idreg)
            villeMod = elem['ville'].replace("'"," ")
            ins = "insert into ville (id,name,code_postal,id_region) values ('"+index+"',"+"'"+villeMod+"',"+"'"+elem['codepostal']+"',"+"'"+idreg+"')"
            ins = ins.encode('ascii', 'ignore').decode('ascii')
            try:
                self.connection.execute(ins)
            except:
                print("Errore: "+ins)
            i = i + 1
        self.conn.commit()

    def showTable(self,tableName):
        self.connection.execute('select * from '+tableName)  # use triple quotes if you want to spread your query across multiple lines
        for row in self.connection:
            print(row[0], '-', row[1],'-',row[2],'-',row[3])  # this only shows the first two columns, to add an additional column you'll need to add , '-', row[2], etc.
        # conn.close()