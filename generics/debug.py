import os
from datetime import datetime
 
class Debug:
    def __init__(self):      
        self.dirName = 'logs'
        if not os.path.exists(self.dirName):
            os.mkdir(self.dirName)
            
    def record(self, channel, message ):
        now = datetime.now()
        date_time = now.strftime("%m-%d-%Y")
        folderName = self.dirName+ '/'+date_time +'/'+ channel
        
        try:
            os.makedirs(folderName)                
        except FileExistsError:
            pass
        
        f = open(folderName + "/log.txt", "a")
        print('['+channel.upper()+']', now.strftime("%m/%d/%Y - %H:%M:%S") + ' -> ' + message)
        f.write(now.strftime("%m/%d/%Y - %H:%M:%S") + ' -> ' + message+'\n')
        f.close()        
    

debug = Debug()    
    