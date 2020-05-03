import sqlalchemy as sal
from sqlalchemy import create_engine
import pandas as pd
from credential import usrnm, pwd, host, db

engine = sal.create_engine('postgresql://' +usrnm('vn')+':'+pwd('vn')+'@'+host()+':5432/'+db('vn'))
conn = engine.connect()

command = """select count("userID"), "activeDate" from user_active_events group by 2 """
dau = pd.DataFrame(engine.execute(command), columns = ['DAU', 'activeDate'])
print(dau)
conn.close()