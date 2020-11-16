import math
import matplotlib.pyplot as plt
import keras
import pandas as pd
import numpy as np

import os
from fbprophet import Prophet



prueba=pd.read_csv('C:/Users/ALEXANDRA/Documents/DS4A/case_1.4_final/clean_teate_05nov.csv')

prueba['month_year']=pd.to_datetime(prueba['Fecha Pedido']).dt.to_period('M')

prueba_slice=pd.DataFrame(prueba.groupby(['month_year', 'Población def', 'Nombre Fabricante'])['Pedido'].nunique())
prueba_slice.reset_index(inplace=True)

prueba_slice.sort_values(by=['Población def', 'month_year', 'Pedido'], ascending=[True, True, False], inplace=True)


prueba_slice.reset_index(inplace=True)
del prueba_slice['index']
prueba_slice.head(50)


prueba_slicecali=prueba_slice[prueba_slice['Población def']=='Cali']
prueba_slicecali1=prueba_slicecali[prueba_slicecali['month_year']=='2019-01'][:10]
prueba_slicecali2=prueba_slicecali[prueba_slicecali['month_year']=='2019-02'][:10]
prueba_slicecali3=prueba_slicecali[prueba_slicecali['month_year']=='2019-03'][:10]
prueba_slicecali4=prueba_slicecali[prueba_slicecali['month_year']=='2019-04'][:10]
prueba_slicecali5=prueba_slicecali[prueba_slicecali['month_year']=='2019-05'][:10]
prueba_slicecali6=prueba_slicecali[prueba_slicecali['month_year']=='2019-06'][:10]
prueba_slicecali7=prueba_slicecali[prueba_slicecali['month_year']=='2019-07'][:10]
prueba_slicecali8=prueba_slicecali[prueba_slicecali['month_year']=='2019-08'][:10]
prueba_slicecali9=prueba_slicecali[prueba_slicecali['month_year']=='2019-09'][:10]
prueba_slicecali10=prueba_slicecali[prueba_slicecali['month_year']=='2019-10'][:10]
prueba_slicecali11=prueba_slicecali[prueba_slicecali['month_year']=='2019-11'][:10]
prueba_slicecali12=prueba_slicecali[prueba_slicecali['month_year']=='2019-12'][:10]
prueba_slicecali13=prueba_slicecali[prueba_slicecali['month_year']=='2020-01'][:10]
prueba_slicecali14=prueba_slicecali[prueba_slicecali['month_year']=='2020-02'][:10]
prueba_slicecali15=prueba_slicecali[prueba_slicecali['month_year']=='2020-03'][:10]
prueba_slicecali16=prueba_slicecali[prueba_slicecali['month_year']=='2020-04'][:10]
prueba_slicecali17=prueba_slicecali[prueba_slicecali['month_year']=='2020-05'][:10]
prueba_slicecali18=prueba_slicecali[prueba_slicecali['month_year']=='2020-06'][:10]
prueba_slicecali19=prueba_slicecali[prueba_slicecali['month_year']=='2020-07'][:10]
prueba_slicecali20=prueba_slicecali[prueba_slicecali['month_year']=='2020-08'][:10] 


prueba_slicemed=prueba_slice[prueba_slice['Población def']=='Medellin']

prueba_slicemed8=prueba_slicemed[prueba_slicemed['month_year']=='2019-08'][:10]
prueba_slicemed9=prueba_slicemed[prueba_slicemed['month_year']=='2019-09'][:10]
prueba_slicemed10=prueba_slicemed[prueba_slicemed['month_year']=='2019-10'][:10]
prueba_slicemed11=prueba_slicemed[prueba_slicemed['month_year']=='2019-11'][:10]
prueba_slicemed12=prueba_slicemed[prueba_slicemed['month_year']=='2019-12'][:10]
prueba_slicemed13=prueba_slicemed[prueba_slicemed['month_year']=='2020-01'][:10]
prueba_slicemed14=prueba_slicemed[prueba_slicemed['month_year']=='2020-02'][:10]
prueba_slicemed15=prueba_slicemed[prueba_slicemed['month_year']=='2020-03'][:10]
prueba_slicemed16=prueba_slicemed[prueba_slicemed['month_year']=='2020-04'][:10]
prueba_slicemed17=prueba_slicemed[prueba_slicemed['month_year']=='2020-05'][:10]
prueba_slicemed18=prueba_slicemed[prueba_slicemed['month_year']=='2020-06'][:10]
prueba_slicemed19=prueba_slicemed[prueba_slicemed['month_year']=='2020-07'][:10]
prueba_slicemed20=prueba_slicemed[prueba_slicemed['month_year']=='2020-08'][:10]


pruebatotal=pd.concat([prueba_slicecali1, prueba_slicecali2, prueba_slicecali3, prueba_slicecali4, prueba_slicecali5, prueba_slicecali6, prueba_slicecali7, prueba_slicecali8, prueba_slicecali9,
                      prueba_slicecali10, prueba_slicecali11, prueba_slicecali12, prueba_slicecali13, prueba_slicecali14, prueba_slicecali15, prueba_slicecali16, prueba_slicecali17, prueba_slicecali18,
                      prueba_slicecali19, prueba_slicecali20])
                      
pruebatotalmed= pd.concat([prueba_slicemed8, prueba_slicemed9, prueba_slicemed10, prueba_slicemed11, prueba_slicemed12, prueba_slicemed13, prueba_slicemed14, prueba_slicemed15,
prueba_slicemed16, prueba_slicemed17, prueba_slicemed18, prueba_slicemed19, prueba_slicemed20])

total=pd.concat([pruebatotal, pruebatotalmed])

total.to_csv('slicer.csv')

###it was sent to RDS


import pandas as pd
import numpy as np
import re
import io
from unicodedata import normalize
from sqlalchemy import create_engine

host = 'dbteate.cbxkjvwmbqol.us-east-2.rds.amazonaws.com'
port = 5432
user = 'postgres'
password = 'teate1234'
database = 'postgres'

connDB = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
conn = connDB.raw_connection()
cur = conn.cursor()

SQL_Query= pd.read_sql('SELECT * FROM slicer', connDB)

SQL_Query.head()
import datetime as dt

SQL_Query['monthyear']=pd.to_datetime(SQL_Query['monthyear'])

SQL_Query.head()
import datetime as dt

SQL_Query['monthyear']=pd.to_datetime(SQL_Query['monthyear'])

SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("TORRECAFE AGUILA ROJA & CIA S.A.","AGUILA ROJA")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("BRINSA S.A.","BRINSA")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("ARROZ FLORHUILA S.A","FLORHUILA")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("COMESTIBLES ALDOR S.A.S","ALDOR")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("HARINERA DEL VALLE S.A","HARINERA VALLE")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("MONDELEZ COLOMBIA S.A.S","MONDELEZ")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("REFISAL","REFISAL")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("PROCTER & GAMBLE COLOMBIA LTDA","PROCTER GAMBLE")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("INDUSTRIA NACIONAL DE GASEOSAS S.A.","IND NAL GASEOSAS")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("COLGATE PALMOLIVE COMPAÑIA","COLGATE")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("GRANISAL LTDA","GRANISAL")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("LIDERPAN S.A","LIDERPAN")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("TECNOQUIMICAS S.A.","TECNOQUIMICAS")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("EMPAQUETADOS EL TRECE S.A.S","EMPAQUETADOS")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("PRODUCTORA Y COMERCIALIZADORA M.J.G","COMERC M.J.G")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("KELLOGGS DE COLOMBIA S.A","KELLOGS")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("DETERGENTES LTDA.","DETERGENTES")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("INDUSTRIAS PATOJITO S.A.S","PATOJITO ")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("ARROCERA LA ESMERALDA S.A.S","LA ESMERALDA")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("MAYAGÜEZ S.A","MAYAGUEZ")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("COMERCIALIZADORA PROCON S.A","PROCON")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("PRODUCTOS YUPI S.A.S","YUPI")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("COLOMBINA S.A","COLOMBINA")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("ORGANIZACION ROA S.A","ROA")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("RÁPIDOS Y SABROSOS DE COLOMBIA S.A","RAPIDOS Y FURIOSOS")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("AGROINDUSTRIAL MOLINO SONORA A.P S.","MOLINO SONORA")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("COMPAÑIA NACIONAL DE LEVADURAS","COMP LEVADURAS")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("GASEOSAS POSADA TOBON S.A.","POSTOBON")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("JOHNSON & JOHNSON DE COLOMBIA S.A","J&J COL")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("Gillette","GILLETE")
SQL_Query['nombrefabricante']=SQL_Query['nombrefabricante'].replace("CASA LUKER S.A","CASA LUKER")

import plotly.express as px
fig=px.bar(SQL_Query, x=SQL_Query[SQL_Query['poblaciondef']=='Cali']['nombrefabricante'], y=SQL_Query[SQL_Query['poblaciondef']=='Cali']['pedido'])
          
fig.update_layout(xaxis={'categoryorder':'total descending'})


                 
