import sqlalchemy as sal
from sqlalchemy import create_engine
from credential import usrnm, pwd, host, db
import pandas as pd

engine = sal.create_engine('postgresql://' +usrnm('vn')+':'+pwd('vn')+'@'+host()+':5432/'+db('vn'))
conn = engine.connect()

command = """select "userID", min("activeDate") from user_active_events group by 1 """
df = pd.DataFrame(engine.execute(command), columns = ['NRU','activeDate'])
df_group = df.groupby(["activeDate"]).count()
print(df_group.sort_values(by = ['activeDate'], ascending = [True]))

conn.close()