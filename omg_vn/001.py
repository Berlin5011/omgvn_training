import sqlalchemy as sal
from sqlalchemy import create_engine
from credential import usrnm, pwd, host, db
import pandas as pd

engine = sal.create_engine('postgresql://'+ usrnm('vn') +':'+pwd('vn')+'@'+host()+':5432/'+db('vn'))
conn = engine.connect()

command = """select "userID","chargeDate", "grossRev" from dbg_add"""
df = pd.DataFrame(engine.execute(command), columns = ['userID', 'chargeDate', 'grossRev'])
result = df.groupby(["userID","chargeDate"]).sum()
result_sort = result.sort_values(by = ['chargeDate', 'userID'], ascending = [True, True])
print(result_sort)
conn.close()