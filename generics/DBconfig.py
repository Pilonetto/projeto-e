import configparser

class Settings:
    def __init__(self):        
        config =  configparser.ConfigParser()
        config.read("settings.ini")
        
        dConfig = config.__dict__['_sections'].copy()
        
        self.PBDSserver = dConfig['PBDS']['serverpbds']
        self.PBDSport = dConfig['PBDS']['portpbds']
        self.PBDSservername = dConfig['PBDS']['servernamepbds']
        try:
            self.PBDSpw = dConfig['PBDS']['pw']
        except:
            self.PBDSpw = 'XPT2000'
        #self.Instance = dConfig['PBDS']['instance']
                           
        self.server = dConfig['db']['server']
        self.port = dConfig['db']['port']
        self.name = dConfig['db']['name']
        try:
            self.DBpw = dConfig['db']['pw']
        except:
            self.DBpw = 'XPT2000'        
        #self.InstanceS1 = dConfig['db']['instance1']
        
        self.server2 = dConfig['db']['server2']
        self.port2 = dConfig['db']['port2']
        self.name2 = dConfig['db']['name2']
        #self.InstanceS2 = dConfig['db']['instance2']
        
    def GetDBServer(self):
        return self.server
    
    def GetDBServer2(self):
        return self.server2
    
    def GetDBPort(self):
        return self.port    
    
    def GetDBPort2(self):
        return self.port2 
    
    def GetPBDSserver(self):
        return self.serverPBDS
    
    def GetPBDSservername(self):
        return self.servernamePBDS    
    
    def GetPBDSport(self):
        return self.portPBDS  
    
    def GetDBname(self):
        return self.name
    
    def GetDBname2(self):
        return self.name2
    