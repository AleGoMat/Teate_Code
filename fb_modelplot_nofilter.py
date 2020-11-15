

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

SQL_Query= pd.read_sql('SELECT * FROM meanmedellin', connDB)
SQL_QueryCAL= pd.read_sql('SELECT * FROM meanmonthcal', connDB)

SQL_Query=SQL_Query.replace('NULL', np.nan)
SQL_QueryCAL=SQL_QueryCAL.replace('NULL', np.nan)

SQL_Query['arrozmedellin'] = pd.to_numeric(SQL_Query['arrozmedellin'])
SQL_Query['aseopersomedellin'] = pd.to_numeric(SQL_Query['aseopersomedellin'])
SQL_Query['despensamedellin'] = pd.to_numeric(SQL_Query['despensamedellin'])
SQL_Query['aseohogarmedellin'] = pd.to_numeric(SQL_Query['aseohogarmedellin'])
SQL_Query['confiteriamedellin'] = pd.to_numeric(SQL_Query['confiteriamedellin'])
SQL_Query['bebidasmedellin'] = pd.to_numeric(SQL_Query['bebidasmedellin'])
SQL_Query['otrosmedellin'] = pd.to_numeric(SQL_Query['otrosmedellin'])

SQL_Query['ASEOPERSO_MEDELLIN']=SQL_Query['aseopersomedellin']
SQL_Query['ASEOHOGAR_MEDELLIN']=SQL_Query['aseohogarmedellin']
SQL_Query['CONFITERIA_MEDELLIN']=SQL_Query['confiteriamedellin']
SQL_Query['BEBIDAS_MEDELLIN']=SQL_Query['bebidasmedellin']
SQL_Query['ARROZ_MEDELLIN']=SQL_Query['arrozmedellin']
SQL_Query['DESPENSA_MEDELLIN']=SQL_Query['despensamedellin']
SQL_Query['OTROS_MEDELLIN']=SQL_Query['otrosmedellin']

SQL_QueryCAL['arrozmedellin'] = pd.to_numeric(SQL_QueryCAL['arrozmedellin'])
SQL_QueryCAL['aseopersomedellin'] = pd.to_numeric(SQL_QueryCAL['aseopersomedellin'])
SQL_QueryCAL['despensamedellin'] = pd.to_numeric(SQL_QueryCAL['despensamedellin'])
SQL_QueryCAL['aseohogarmedellin'] = pd.to_numeric(SQL_QueryCAL['aseohogarmedellin'])
SQL_QueryCAL['confiteriamedellin'] = pd.to_numeric(SQL_QueryCAL['confiteriamedellin'])
SQL_QueryCAL['bebidasmedellin'] = pd.to_numeric(SQL_QueryCAL['bebidasmedellin'])
SQL_QueryCAL['otrosmedellin'] = pd.to_numeric(SQL_QueryCAL['otrosmedellin'])

SQL_QueryCAL['ASEOPERSO_CALI']=SQL_QueryCAL['aseopersomedellin']
SQL_QueryCAL['ASEOHOGAR_CALI']=SQL_QueryCAL['aseohogarmedellin']
SQL_QueryCAL['CONFITERIA_CALI']=SQL_QueryCAL['confiteriamedellin']
SQL_QueryCAL['BEBIDAS_CALI']=SQL_QueryCAL['bebidasmedellin']
SQL_QueryCAL['ARROZ_CALI']=SQL_QueryCAL['arrozmedellin']
SQL_QueryCAL['DESPENSA_CALI']=SQL_QueryCAL['despensamedellin']
SQL_QueryCAL['OTROS_CALI']=SQL_QueryCAL['otrosmedellin']

del SQL_Query['otrosmedellin'] 
del SQL_Query['despensamedellin']
del SQL_Query['arrozmedellin']
del SQL_Query['bebidasmedellin']
del SQL_Query['confiteriamedellin']
del SQL_Query['aseohogarmedellin']
del SQL_Query['aseopersomedellin']

del SQL_QueryCAL['otrosmedellin'] 
del SQL_QueryCAL['despensamedellin']
del SQL_QueryCAL['arrozmedellin']
del SQL_QueryCAL['bebidasmedellin']
del SQL_QueryCAL['confiteriamedellin']
del SQL_QueryCAL['aseohogarmedellin']
del SQL_QueryCAL['aseopersomedellin']

meanmonth6=SQL_Query.merge(SQL_QueryCAL, how='left', on='monthyear')
meanmonth6.set_index(meanmonth6['monthyear'], inplace=True)
del meanmonth6['monthyear']

import pandas as pd
import numpy as np
import re
import io
from unicodedata import normalize
from sqlalchemy import create_engine
import seaborn as sns

host = 'dbteate.cbxkjvwmbqol.us-east-2.rds.amazonaws.com'
port = 5432
user = 'postgres'
password = 'teate1234'
database = 'postgres'

connDB = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
conn = connDB.raw_connection()
cur = conn.cursor()

SQL_Query= pd.read_sql('SELECT * FROM forecast', connDB)
SQL_QueryCAL= pd.read_sql('SELECT * FROM forecastcal', connDB)

SQL_Query=SQL_Query.replace('NULL', np.nan)
SQL_QueryCAL=SQL_QueryCAL.replace('NULL', np.nan)

SQL_Query['arrozmedellin'] = pd.to_numeric(SQL_Query['arrozmedellin'])
SQL_Query['aseopersomedellin'] = pd.to_numeric(SQL_Query['aseopersomedellin'])
SQL_Query['despensamedellin'] = pd.to_numeric(SQL_Query['despensamedellin'])
SQL_Query['aseohogarmedellin'] = pd.to_numeric(SQL_Query['aseohogarmedellin'])
SQL_Query['confiteriamedellin'] = pd.to_numeric(SQL_Query['confiteriamedellin'])
SQL_Query['bebidasmedellin'] = pd.to_numeric(SQL_Query['bebidasmedellin'])
SQL_Query['otrosmedellin'] = pd.to_numeric(SQL_Query['otrosmedellin'])
SQL_Query['ASEOPERSO_MEDELLIN']=SQL_Query['aseopersomedellin']
SQL_Query['ASEOHOGAR_MEDELLIN']=SQL_Query['aseohogarmedellin']
SQL_Query['CONFITERIA_MEDELLIN']=SQL_Query['confiteriamedellin']
SQL_Query['BEBIDAS_MEDELLIN']=SQL_Query['bebidasmedellin']
SQL_Query['ARROZ_MEDELLIN']=SQL_Query['arrozmedellin']
SQL_Query['DESPENSA_MEDELLIN']=SQL_Query['despensamedellin']
SQL_Query['OTROS_MEDELLIN']=SQL_Query['otrosmedellin']

SQL_QueryCAL['arrozmedellin'] = pd.to_numeric(SQL_QueryCAL['arrozmedellin'])
SQL_QueryCAL['aseopersomedellin'] = pd.to_numeric(SQL_QueryCAL['aseopersomedellin'])
SQL_QueryCAL['despensamedellin'] = pd.to_numeric(SQL_QueryCAL['despensamedellin'])
SQL_QueryCAL['aseohogarmedellin'] = pd.to_numeric(SQL_QueryCAL['aseohogarmedellin'])
SQL_QueryCAL['confiteriamedellin'] = pd.to_numeric(SQL_QueryCAL['confiteriamedellin'])
SQL_QueryCAL['bebidasmedellin'] = pd.to_numeric(SQL_QueryCAL['bebidasmedellin'])
SQL_QueryCAL['otrosmedellin'] = pd.to_numeric(SQL_QueryCAL['otrosmedellin'])

SQL_QueryCAL['ASEOPERSO_CALI']=SQL_QueryCAL['aseopersomedellin']
SQL_QueryCAL['ASEOHOGAR_CALI']=SQL_QueryCAL['aseohogarmedellin']
SQL_QueryCAL['CONFITERIA_CALI']=SQL_QueryCAL['confiteriamedellin']
SQL_QueryCAL['BEBIDAS_CALI']=SQL_QueryCAL['bebidasmedellin']
SQL_QueryCAL['ARROZ_CALI']=SQL_QueryCAL['arrozmedellin']
SQL_QueryCAL['DESPENSA_CALI']=SQL_QueryCAL['despensamedellin']
SQL_QueryCAL['OTROS_CALI']=SQL_QueryCAL['otrosmedellin']

del SQL_Query['otrosmedellin'] 
del SQL_Query['despensamedellin']
del SQL_Query['arrozmedellin']
del SQL_Query['bebidasmedellin']
del SQL_Query['confiteriamedellin']
del SQL_Query['aseohogarmedellin']
del SQL_Query['aseopersomedellin']

del SQL_QueryCAL['otrosmedellin'] 
del SQL_QueryCAL['despensamedellin']
del SQL_QueryCAL['arrozmedellin']
del SQL_QueryCAL['bebidasmedellin']
del SQL_QueryCAL['confiteriamedellin']
del SQL_QueryCAL['aseohogarmedellin']
del SQL_QueryCAL['aseopersomedellin']

jh_forecast6=SQL_Query.merge(SQL_QueryCAL, how='left', on='ds')
jh_forecast6.set_index(jh_forecast6['ds'], inplace=True)
jh_forecast6.head()
del jh_forecast6['ds']


SQL_Query= pd.read_sql('SELECT * FROM realmedellin', connDB)
SQL_QueryCAL= pd.read_sql('SELECT * FROM realcal', connDB)

SQL_Query=SQL_Query.replace('NULL', np.nan)
SQL_QueryCAL=SQL_Query.replace('NULL', np.nan)

SQL_Query

SQL_Query['arrozmedellin'] = pd.to_numeric(SQL_Query['arrozmedellin'])
SQL_Query['aseopersomedellin'] = pd.to_numeric(SQL_Query['aseopersomedellin'])
SQL_Query['despensamedellin'] = pd.to_numeric(SQL_Query['despensamedellin'])
SQL_Query['aseohogarmedellin'] = pd.to_numeric(SQL_Query['aseohogarmedellin'])
SQL_Query['confiteriamedellin'] = pd.to_numeric(SQL_Query['confiteriamedellin'])
SQL_Query['bebidasmedellin'] = pd.to_numeric(SQL_Query['bebidasmedellin'])
SQL_Query['otrosmedellin'] = pd.to_numeric(SQL_Query['otrosmedellin'])
SQL_Query['ASEOPERSO_MEDELLIN']=SQL_Query['aseopersomedellin']
SQL_Query['ASEOHOGAR_MEDELLIN']=SQL_Query['aseohogarmedellin']
SQL_Query['CONFITERIA_MEDELLIN']=SQL_Query['confiteriamedellin']
SQL_Query['BEBIDAS_MEDELLIN']=SQL_Query['bebidasmedellin']
SQL_Query['ARROZ_MEDELLIN']=SQL_Query['arrozmedellin']
SQL_Query['DESPENSA_MEDELLIN']=SQL_Query['despensamedellin']
SQL_Query['OTROS_MEDELLIN']=SQL_Query['otrosmedellin']
del SQL_Query['otrosmedellin'] 
del SQL_Query['despensamedellin']
del SQL_Query['arrozmedellin']
del SQL_Query['bebidasmedellin']
del SQL_Query['confiteriamedellin']
del SQL_Query['aseohogarmedellin']
del SQL_Query['aseopersomedellin']

SQL_QueryCAL['arrozmedellin'] = pd.to_numeric(SQL_QueryCAL['arrozmedellin'])
SQL_QueryCAL['aseopersomedellin'] = pd.to_numeric(SQL_QueryCAL['aseopersomedellin'])
SQL_QueryCAL['despensamedellin'] = pd.to_numeric(SQL_QueryCAL['despensamedellin'])
SQL_QueryCAL['aseohogarmedellin'] = pd.to_numeric(SQL_QueryCAL['aseohogarmedellin'])
SQL_QueryCAL['confiteriamedellin'] = pd.to_numeric(SQL_QueryCAL['confiteriamedellin'])
SQL_QueryCAL['bebidasmedellin'] = pd.to_numeric(SQL_QueryCAL['bebidasmedellin'])
SQL_QueryCAL['otrosmedellin'] = pd.to_numeric(SQL_QueryCAL['otrosmedellin'])
SQL_QueryCAL['ASEOPERSO_CALI']=SQL_QueryCAL['aseopersomedellin']
SQL_QueryCAL['ASEOHOGAR_CALI']=SQL_QueryCAL['aseohogarmedellin']
SQL_QueryCAL['CONFITERIA_CALI']=SQL_QueryCAL['confiteriamedellin']
SQL_QueryCAL['BEBIDAS_CALI']=SQL_QueryCAL['bebidasmedellin']
SQL_QueryCAL['ARROZ_CALI']=SQL_QueryCAL['arrozmedellin']
SQL_QueryCAL['DESPENSA_CALI']=SQL_QueryCAL['despensamedellin']
SQL_QueryCAL['OTROS_CALI']=SQL_QueryCAL['otrosmedellin']
del SQL_QueryCAL['otrosmedellin'] 
del SQL_QueryCAL['despensamedellin']
del SQL_QueryCAL['arrozmedellin']
del SQL_QueryCAL['bebidasmedellin']
del SQL_QueryCAL['confiteriamedellin']
del SQL_QueryCAL['aseohogarmedellin']
del SQL_QueryCAL['aseopersomedellin']


real6=SQL_Query.merge(SQL_QueryCAL, how='left', on='fechapedido')
real6.set_index(real6['fechapedido'], inplace=True)
del real6['fechapedido']




import plotly.graph_objects as go 

fig = go.Figure()

for column in jh_forecast6.columns.to_list():
    fig.add_trace(
        go.Scatter(
            x = jh_forecast6.index,
            y = jh_forecast6[column],
            name = 'yhat'
        )
    )
    
for column in meanmonth6.columns.to_list():
    fig.add_trace(
        go.Scatter(
            x = meanmonth6.index,
            y = meanmonth6[column],
            name = 'y_mean'
        )
    )
    
for column in real6.columns.to_list():
    fig.add_trace(
        go.Scatter(
            x = real6.index,
            y = real6[column],
            name = 'y_real'
        )
    )
    
fig.update_layout(
    updatemenus=[go.layout.Updatemenu(
        active=0,
        buttons=list(
            [dict(label = 'ALL',
                  
             method = 'update',
             args = [{'visible': [True, True, True, True, True, True, False]}, # the index of True aligns with the indices of plot traces
                          {'title': 'ALL',
                           'showlegend':True}]),
             
             dict(label = 'ARROZ_MEDELLIN',
                  
             method = 'update',
             args = [{'visible': [True, False, False, False, False, False, False]}, # the index of True aligns with the indices of plot traces
                          {'title': 'ARROZ_MEDELLIN',
                           'showlegend':True}]),
             dict(label = 'DESPENSA_MEDELLIN',
                  method = 'update',
                  args = [{'visible': [False, True, False, False, False, False, False]},
                          {'title': 'DESPENSA_MEDELLIN',
                           'showlegend':True}]),
             dict(label = 'ASEOPERSO_MEDELLIN',
                  method = 'update',
                  args = [{'visible': [False, False, True, False, False, False, False]},
                          {'title': 'ASEOPERSO_MEDELLIN',
                           'showlegend':True}]),
             dict(label = 'ASEOHOGAR_MEDELLIN',
                  method = 'update',
                  args = [{'visible': [False, False, False, True, False, False, False]},
                          {'title': 'ASEOHOGAR_MEDELLIN',
                           'showlegend':True}]),
             dict(label = 'CONFITERIA_MEDELLIN',
                  method = 'update',
                  args = [{'visible': [False, False, False, False, True, False, False]},
                          {'title': 'CONFITERIA_MEDELLIN',
                           'showlegend':True}]),
             dict(label = 'BEBIDAS_MEDELLIN',
                  method = 'update',
                  args = [{'visible': [False, False, False, False, False, True, False]},
                          {'title': 'BEBIDAS_MEDELLIN',
                           'showlegend':True}]),
             dict(label = 'OTROS_MEDELLIN',
                  method = 'update',
                  args = [{'visible': [False, False, False, False, False, False, True]},
                          {'title': 'OTROS_MEDELLIN',
                           'showlegend':True}]),
             ])
        )
    ])

fig.show()
