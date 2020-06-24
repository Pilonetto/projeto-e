# -*- coding: utf-8 -*-
"""
Created on Fri Jun 19 14:59:27 2020

@author: marlon
"""
import pandas as pd

class Sales:
    def __init__(self, db):   
        self.connected = 0 
        self.db = db
    
    def save(self, df, companyId):
        print('Save sales')
        
        std = '''INSERT INTO Sales (ExCompanyId,CompanyCode, OrderNumber, 
                                     Employee, Product, ProductCategory, Client, 
                                     ClientCity, SaleDateTime, ProductCost, 
                                     DiscountAmount,Amount, Total,  FormPayment)
        VALUES (%d, '%s', %d, '%s','%s','%s','%s','%s','%s',%.2f,%.2f,%.2f,%.2f,'%s')'''
        for index, row in df.iterrows():       
            self.db.InsertUpdate(std % (int(companyId), 
                                        row['Company Code'], 
                                        int(row['Order Number']), 
                                        row['Employee'], 
                                        row['Product Category'], 
                                        row['Product'],
                                        row['Client'],
                                        row['Client City'],
                                        row['Date'],
                                        row['Product Cost'],
                                        row['Discount Amount'],
                                        row['Amount'],
                                        row['Total'],
                                        str(row['Form of payment'])))
            
    def generateMetrics(self, companyId):
        print('Genereta company metrics')
        df = pd.DataFrame(pd.read_sql_query(''' SELECT *, month([SaleDateTime]) as smonth, year([SaleDateTime]) as syear  
                                            FROM Sales WHERE ExCompanyId = %d ''' % (companyId),self.db.connection)) 

        gbyear = df.groupby('syear')            
        for yearkey in gbyear.groups.keys():
            gbyearkey = gbyear.get_group(yearkey)    
            gbmonth = gbyearkey.groupby('smonth')                                                                               #em todos os inserts
            for monthkey in gbmonth.groups.keys():                    
                gbmonthkey = gbmonth.get_group(monthkey)
                    
                total = gbmonthkey['Total'].sum() 
                amount = gbmonthkey['Total'].sum()
                sumsales = gbmonthkey.groupby('OrderNumber').Total.sum()      
                mtkt = sumsales.sum() / len(sumsales)
                ordercount =  len(sumsales)
                stddel = ''' DELETE FROM MonthlyMetrics WHERE ExCompanyId = %d AND Month = %d AND Year = %d'''
                stdins = ''' INSERT INTO MonthlyMetrics (ExCompanyId,Month, Year, ItemsSold, TotalSales,	MediumTicket, OrderCount)
                              VALUES (%d, %d, %d, %.2f, %.2f, %.2f, %d)'''
                    
                self.db.InsertUpdate(stddel % ( companyId, monthkey, yearkey ))   
                self.db.InsertUpdate(stdins % ( companyId, monthkey, yearkey, amount,total, mtkt, ordercount ))                    
        
        
    # Test method 
    def export(self):
        
        sql = ''' SELECT M.ID_MOVPRODUTOS AS [Order Number],IM.ID_ITENSMOVPRODUTOS AS [Item Order Number],  ISNULL(P.NOMEPRODUTO,'No Product') AS [Product], M.DATA AS [Sale Date Time],
                                       IM.ID_FILIAL [Company Code], 
                                       ISNULL(E.NOMEENTIDADE,'No Client') AS [Client], ISNULL(C.NOMECIDADE,'No City') AS [Client City],  ISNULL(F.NOMEFUNCIONARIO,'No Employee') AS [Employee],
									   ISNULL(GP.NOMEGRUPOPRODUTOS,'No Category') AS [Product Category],
                                       
                                       ISNULL(IM.VLRCUSTO,-1) [Product Cost], 
                                       ISNULL(IM.QTDE,-1) AS [Amount],  ISNULL(IM.TOTAL,-1) AS Total , 
                                       isnull(IM.VLRDESCONTO,0) as [Discount Amount], isnull(M.FORMAPGTO,250) as [Form of payment] 
                                       FROM ITENSMOVPRODUTOS IM
                                       INNER JOIN MOVPRODUTOS M ON IM.ID_MOVPRODUTOS = M.ID_MOVPRODUTOS 
                                 		  and IM.ID_FILIAL = M.ID_FILIAL 
                                           AND IM.ID_DB = M.ID_DB
                                       INNER JOIN COMPROVANTES ON M.ID_COMPROVANTE = COMPROVANTES.ID_COMPROVANTE
  									   AND M.ID_Filial = COMPROVANTES.ID_Filial 
									   AND M.ID_DB = COMPROVANTES.ID_DB

									   INNER JOIN PRODUTOS P ON IM.ID_PRODUTOS = P.ID_PRODUTOS AND IM.ID_FILIAL = P.ID_FILIAL

									   INNER JOIN ENTIDADES E ON M.ID_ENTIDADE = E.ID_ENTIDADE AND IM.ID_FILIAL = E.ID_FILIAL
									   INNER JOIN CIDADES C ON C.ID_CIDADES = E.ID_CIDADES AND C.ID_FILIAL = E.ID_FILIAL
									   INNER JOIN FUNCIONARIOS F ON F.ID_FUNCIONARIOS = IM.ID_FUNCIONARIOS AND F.ID_FILIAL = IM.ID_FILIAL
									   INNER JOIN GRUPOPRODUTOS GP ON GP.ID_GRUPOPRODUTOS = P.ID_GRUPOPRODUTOS AND GP.ID_FILIAL = P.ID_FILIAL 

									   LEFT JOIN COMPDEVOLUCAO ON COMPROVANTES.ID_FILIAL = COMPDEVOLUCAO.ID_FILIAL
									   AND COMPROVANTES.ID_COMPROVANTE = COMPDEVOLUCAO.ID_COMPROVANTE AND COMPROVANTES.ID_DB = COMPDEVOLUCAO.ID_DB

									   AND IM.ID_DB = M.ID_DB
                                       
                                       WHERE MONTH(M.DTACONTA)= 1 and YEAR(M.DTACONTA) = 2020 AND IM.STATUS = 1 AND M.SAIDAS_ENTRADAS = 0 
                                       AND COMPDEVOLUCAO.ID_COMPROVANTE IS NULL ''' 
                                       
                                       
        df = pd.DataFrame(pd.read_sql_query(sql,self.db.connection))                               
        df.to_csv('sales.csv', sep=';', header=True, decimal= ',')    
        print('vendas exportadas')                                   