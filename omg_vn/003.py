import sqlalchemy as sal
from sqlalchemy import create_engine
from credential import usrnm, pwd, host, db
import pandas as pd

engine = sal.create_engine('postgresql://' + usrnm('vn') + ':' + pwd('vn') +'@'+host()+':5432/'+db('vn'))
conn = engine.connect()
command = """select "userID", sum("grossRev") from dbg_add group by 1""" 
df = pd.DataFrame(engine.execute(command), columns = ['userID', 'Rev'])
df.loc[df['Rev']<1000000, 'Ranking'] = 'non-Vip'
df.loc[df['Rev'].between(1000000,9999999), 'Ranking'] = 'Vip0'
df.loc[df['Rev'].between(10000000,19999999), 'Ranking'] = 'Vip1'
df.loc[df['Rev'].between(20000000,29999999), 'Ranking'] = 'Vip2'
df.loc[df['Rev'].between(30000000,39999999), 'Ranking'] = 'Vip3'
df.loc[df['Rev'].between(40000000,49999999), 'Ranking'] = 'Vip4'
df.loc[df['Rev'].between(50000000,59999999), 'Ranking'] = 'Vip5'
df.loc[df['Rev'].between(60000000,69999999), 'Ranking'] = 'Vip6'
df.loc[df['Rev'].between(70000000,79999999), 'Ranking'] = 'Vip7'
df.loc[df['Rev'].between(80000000,89999999), 'Ranking'] = 'Vip8'
df.loc[df['Rev'].between(90000000,99999999), 'Ranking'] = 'Vip9'
df.loc[df['Rev']>=100000000, 'Ranking'] = 'Vip10'
print(df.sort_values(by = ['Rev'], ascending = [True]))
conn.close()