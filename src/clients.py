# -*- coding: utf-8 -*-
"""
Created on Mon Jun 22 13:34:06 2020

@author: marlon
"""

import pandas as pd

from sklearn.cluster import KMeans

class Clients:
    def __init__(self, db):   
        self.connected = 0 
        self.db = db
        

    #function for ordering cluster numbers
    def order_cluster(self,cluster_field_name, target_field_name,df,ascending):
        df_new = df.groupby(cluster_field_name)[target_field_name].mean().reset_index()
        df_new = df_new.sort_values(by=target_field_name,ascending=ascending).reset_index(drop=True)
        df_new['index'] = df_new.index
        df_final = pd.merge(df,df_new[[cluster_field_name,'index']], on=cluster_field_name)
        df_final = df_final.drop([cluster_field_name],axis=1)
        df_final = df_final.rename(columns={"index":cluster_field_name})
        return df_final

        
    def generateRFM(self, companyID):
        try:           
            #Conexão com a base  
            print('0')
            print('Company ID' + str(companyID))
            try:
                df = pd.DataFrame(pd.read_sql_query(''' SELECT * FROM Sales WHERE ExCompanyId = %d ''' % (companyID),self.db.connection))             
            except:
                print('Erro ao carregar vendas.')
            
            print('33')
            if df.empty:
                print('Sem dados disponíveis, para realizar a segmentação dos clientes')            
            else:
                print('1')
                df['Date'] = pd.to_datetime(df['SaleDateTime'])
                #create a generic user dataframe to keep Client and new segmentation scores
                dfclient = pd.DataFrame(df['Client'].unique())
                dfclient.columns = ['Client']
                print('2')
                #get the max purchase date for each customer and create a dataframe with it
                df_max_purchase = df.groupby('Client').Date.max().reset_index()
                df_max_purchase.columns = ['Client','MaxPurchaseDate']
                print('3')
                #we take our observation point as the max invoice date in our dataset
                df_max_purchase['Recency'] = (df_max_purchase['MaxPurchaseDate'].max() - df_max_purchase['MaxPurchaseDate']).dt.days
                
                #merge this dataframe to our new user dataframe
                dfclient = pd.merge(dfclient, df_max_purchase[['Client','Recency']], on='Client')
                print('4')
                #build 4 clusters for recency and add it to dataframe
                kmeans = KMeans(n_clusters=4)
                kmeans.fit(dfclient[['Recency']])
                dfclient['RecencyCluster'] = kmeans.predict(dfclient[['Recency']])   
                
                dfclient = self.order_cluster('RecencyCluster', 'Recency',dfclient,False)
                
                
                print('5')
                #FREQUENCY
                
                #get order counts for each user and create a dataframe with it
                dffreq = df.groupby('Client').Date.count().reset_index()
                dffreq.columns = ['Client','Frequency']
                
                #add this data to our main dataframe
                dfclient = pd.merge(dfclient, dffreq, on='Client')
                
                #k-means
                kmeans = KMeans(n_clusters=4)
                kmeans.fit(dfclient[['Frequency']])
                dfclient['FrequencyCluster'] = kmeans.predict(dfclient[['Frequency']])
                
                #order the frequency cluster
                dfclient = self.order_cluster('FrequencyCluster', 'Frequency',dfclient,True)
                
                #REVENUE
                #calculate revenue for each customer
                dfrev = df.groupby('Client').Total.sum().reset_index()

                #merge it with our main dataframe
                dfclient = pd.merge(dfclient, dfrev, on='Client')

                #apply clustering
                kmeans = KMeans(n_clusters=4)
                kmeans.fit(dfclient[['Total']])
                dfclient['TotalCluster'] = kmeans.predict(dfclient[['Total']])


                #order the cluster numbers
                dfclient = self.order_cluster('TotalCluster', 'Total',dfclient,True)


                #calculate overall score and use mean() to see details
                dfclient['OverallScore'] = dfclient['RecencyCluster'] + dfclient['FrequencyCluster'] + dfclient['TotalCluster']
                dfclient.groupby('OverallScore')['Recency','Frequency','Total'].mean()
                
                
                dfclient['Segment'] = 'Valor Baixo'
                dfclient.loc[dfclient['OverallScore']>2,'Segment'] = 'Valor Médio' 
                dfclient.loc[dfclient['OverallScore']>4,'Segment'] = 'Valor Alto' 
                
                dfclient['RecencyText'] = 'Inativo à muitíssimo tempo'
                dfclient.loc[dfclient['RecencyCluster'] == 3, 'RecencyText'] = 'Ativo recentemente'
                dfclient.loc[dfclient['RecencyCluster'] == 2, 'RecencyText'] = 'Inativo à pouco tempo'
                dfclient.loc[dfclient['RecencyCluster'] == 1, 'RecencyText'] = 'Inativo à muito tempo'
                
                
                dfclient['FrequencyText'] = 'Frequência Baixíssima'
                dfclient.loc[dfclient['FrequencyCluster'] == 3, 'FrequencyText'] = 'Frequência Alta'
                dfclient.loc[dfclient['FrequencyCluster'] == 2, 'FrequencyText'] = 'Frequência Média'
                dfclient.loc[dfclient['FrequencyCluster'] == 1, 'FrequencyText'] = 'Frequência Baixa'
                
                dfclient['TotalText'] = 'Receita Muito Baixa'
                dfclient.loc[dfclient['TotalCluster'] == 3, 'TotalText'] = 'Receita Alta'
                dfclient.loc[dfclient['TotalCluster'] == 2, 'TotalText'] = 'Receita Média'
                dfclient.loc[dfclient['TotalCluster'] == 1, 'TotalText'] = 'Receita Baixa'
                
                #dfclient.to_csv('rfm.csv', sep=';', header=True, decimal= ',')
                
                print('Save Rfm Clients')
        
                std = '''INSERT INTO clients (ExCompanyId,Client, Recency, 
                                     RecencyCluster, Frequency, FrequencyCluster, Total, 
                                     TotalCluster, OverallScore, Segment, 
                                     RecencyText,FrequencyText, TotalText)
                        VALUES (%d,'%s', %d, %d, %d, %d,%.2f,%d,%d,'%s','%s','%s','%s')'''
        
                for index, row in dfclient.iterrows():       
                    sql = std % (int(companyID), 
                                        row['Client'], 
                                        row['Recency'], 
                                        row['RecencyCluster'], 
                                        row['Frequency'], 
                                        row['FrequencyCluster'],
                                        row['Total'],
                                        row['TotalCluster'],
                                        row['OverallScore'],
                                        row['Segment'],
                                        row['RecencyText'],
                                        row['FrequencyText'],
                                        row['TotalText']
                                        )                    
                    self.db.InsertUpdate(sql)                
                             
                           
                
        except Exception as e:
            print('Erro-> ' + str(e)) 