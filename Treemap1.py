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

SQL_Query= pd.read_sql('SELECT * FROM map', connDB)

Map_input=SQL_Query[['pobsmall', 'comuna', 'valoringresopedido']]
