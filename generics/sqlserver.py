import pyodbc
from generics.debug import debug

class DataBase:
    def __init__(self):   
        self.connected = 0      
        
    def Connect(self, server,port,database, user, pwd, autocommit=False):        
        self.Server = server
        self.Port = port
        #self.Driver = '''Driver={SQL Server};Server=%s;Database=%s;''' % (self.Server,database)  -> Base zoada 
        #self.Driver='''Driver={SQL Server Native Client 11.0};Server=%s,%d;Database=%s;UID=%s;PWD=%s;MARS_Connection=Yes;''' %(self.Server,self.Port,database, user,pwd) 		
        self.Driver='''Driver={SQL Server Native Client 11.0};Server=%s;Database=%s;UID=%s;PWD=%s;MARS_Connection=Yes;''' %(self.Server,database, user,pwd) 		
                
        self.connection = pyodbc.connect(self.Driver)
        self.connection.autocommit = autocommit
        self.connected = 1

    def Disconnect(self):
        self.connected = 0
        self.connection.close()

    def Query(self, statement):
        if (bool(statement and statement.strip())):
            cursor = self.connection.cursor()
            cursor.execute(statement)
            return cursor.fetchall()
        else:
            return 0
        
# =============================================================================
#     def Execute(self, instruction):
#         print(instruction)
#         if (bool(instruction and instruction.strip())):
#             try:
#                 cursor = self.connection.cursor()
#                 cursor.execute(instruction)
#                 cursor.commit()
#                 return cursor.fetchall()
#                 
#             except(KeyboardInterrupt):
#                 exit
#             except:
#                 print('Error in SQL Execute')
#                 cursor.rollback()
#                 return 0            
#         else:
#             return 0
# =============================================================================
        
    def InsertUpdate(self, instruction):
        '''Função para insert/update '''
        #print(instruction)
        if (bool(instruction and instruction.strip())):
            try:
                cursor = self.connection.cursor()
                cursor.execute(instruction)
                cursor.commit()
                return 1
                
            except(KeyboardInterrupt):
                exit
            except Exception as e:                
                debug.record('DataBase', 'Error in SQL Execute -> ' + str(e))
                cursor.rollback()
                return 0            
        else:
            return 0

        
