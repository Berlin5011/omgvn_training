import sqlalchemy as sal
from sqlalchemy import create_engine
from credential import usrnm, pwd, host, db
import pandas as pd

engine = sal.create_engine('postgresql://'+ usrnm('th') +':'+pwd('th')+'@'+host()+':5432/'+db('th'))
conn = engine.connect()

command = """select "userID","chargeDate", "grossRev" from dbg_add"""
df = pd.DataFrame(engine.execute(command), columns = ['userID', 'chargeDate', 'grossRev'])
print(df.head(5))

conn.close()