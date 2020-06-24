# -*- coding: utf-8 -*-
"""
Created on Thu Jun 18 16:04:29 2020

@author: marlon
"""

import mysql.connector
import base64
import pandas as pd
import threading
import time
from generics.sqlserver import DataBase
from generics.DBconfig import Settings
from src.sales import Sales
from src.clients import Clients

from flask_cors import CORS
from flask_restful import Resource, Api
from flask import Flask, redirect, url_for, request


settings = Settings()

db = DataBase()
db.Connect(settings.server,int(settings.port),
             settings.name, 'sa', settings.DBpw)


#Start sales class
sales = Sales(db)
clients = Clients(db)


db = mysql.connector.connect(
  host="162.241.203.10",
  user="treeau37_cli",
  password="Marlon@040826",
  database= 'treeau37_cli',
)
cursor = db.cursor()

_FILENAME = 7
_FILEDATA = 2
_FILEID = 0
_FILECOMPID = 1

_FILESUCESS_01 = 'Arquivo processado com sucesso'
_FILEERROR_110 = 'O arquivo precisa ter 13 colunas.'
_FILEERROR_111 = 'O arquivo está vazio.'
_FILEERROR_112 = 'As colunas [%s] não foram encontradas no arquivo.'
_FILEERROR_113 = 'O formato da data/hora é inválido.'
_FILEERROR_114 = 'Erro ao salvar as vendas.'

threads = list()

def setFileError(id,code, msg):
    sql = ''' UPDATE files SET processed_date=NOW(), state=%d, message='%s' WHERE id = %d ''' % (code,msg,id)
    print(sql)
    cursor.execute(sql)
    db.commit()
    print('File ' + str(id) + ' -> ' + str(code) + ' -> ' + msg)

def stopThreads():
    for i, t in enumerate(threads):
        t.join()
        print('Thread {} Stopped'.format(i))    

def setdtypes(df):
    df['Total'].astype('float32')
    df['Amount'].astype('float32')
    df['Product Cost'].astype('float32')
    df['Amount'].astype('float32')    
    df['Product'].astype('str')    
    df['Company Code'].astype('str')    
    df['Client'].astype('str')    
    df['Client City'].astype('str')  
    df['Employee'].astype('str')  
    df['Product Category'].astype('str') 
    
    return df
    
        
def syncronyze(tempo):
    time.sleep(tempo)
    print('Sincronizando')
    #sales.export()
    cursor.execute("select * from files where state = 0")
    files = cursor.fetchall()
    
    for file in files:
        print('Processando-> ' + file[_FILENAME])
        filedata = base64.b64decode(file[_FILEDATA])
        filename = 'D:/Projeto E/processos/' + file[_FILENAME]  # I assume you have a way of picking unique filenames
        currcompid= file[_FILECOMPID]
        f = open(filename, 'wb')
        f.write(filedata)
        f.close()    
        
        # Load data
        print('Loadind data.')
        #filename = 'D:/Projeto E/processos/Sales2.csv'
        try:
            df = pd.DataFrame(pd.read_csv(filename,sep=';',decimal= ','))
        except Exception:                
            setFileError(int(file[_FILEID]), 109,'Erro ao carregar o arquivo')        
            continue
        try:    
            df = setdtypes(df)
            df['Date'] = pd.to_datetime(df['Sale Date Time'], errors='coerce')
        except Exception:                
            setFileError(int(file[_FILEID]), 113,'Erro ao carregar o arquivo')        
            continue
        
        
        if (len(df.columns) < 13):
            setFileError(int(file[_FILEID]), 110,_FILEERROR_110)
            continue
        
        if (len(df) == 0):
            setFileError(int(file[_FILEID]), 111,_FILEERROR_111)
            continue
        
        ncols = ''
        if 'Company Code' not in df.columns:
            ncols += ' Company Code,'    
        if 'Order Number' not in df.columns:
            ncols += ' Order Number,'      
        if 'Employee' not in df.columns:
            ncols = ' Employee,'   
        if 'Product Category' not in df.columns:
            ncols += ' Product Category,'  
        if 'Product' not in df.columns:
            ncols = ' Product,'          
        if 'Client' not in df.columns:
            ncols += ' Client,'  
        if 'Client City' not in df.columns:
            ncols += ' Client City,'            
        if 'Sale Date Time' not in df.columns:
            ncols += ' Sale Date Time,'
        if 'Product Cost' not in df.columns:
            ncols = ' Product Cost,'            
        if 'Discount Amount' not in df.columns:
            ncols += ' Discount Amount,'          
        if 'Amount' not in df.columns:
            ncols = ' Amount,'            
        if 'Total' not in df.columns:
            ncols += ' Total,'         
        if 'Form of payment' not in df.columns:
            ncols += ' Form of payment,'         
            
        if (len(ncols) > 0):
            ncols = ncols[:-1]
            setFileError(int(file[_FILEID]), 112,(_FILEERROR_112 % (ncols) ))
            continue        
            
        #save sales in sql server
        try:
            sales.save(df,currcompid)
        except:
            setFileError(int(file[_FILEID]), 114,_FILEERROR_114)
            continue            
        
        clients.generateRFM(currcompid)  
        sales.generateMetrics(currcompid)
        
        setFileError(int(file[_FILEID]), 1,_FILESUCESS_01)
    
    x = threading.Thread(target=syncronyze, args=(interval,)) 
    threads.append(x)
    x.start()          


interval = 10 # 5 minutos
x = threading.Thread(target=syncronyze, args=(interval,)) 
threads.append(x)
x.start()    
    
    
app = Flask(__name__)
api = Api(app)
cors = CORS(app, resources={r"/*": {"origins": "*"}})
if __name__ == '__main__':
   app.run(port = 5012,host='0.0.0.0',threaded=True)    
   stopThreads()
   print('Cancelou')

    
    