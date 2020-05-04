import sqlalchemy as sal
from sqlalchemy import create_engine
import pandas as pd
from credential import usrnm, pwd, host, db
from date_mod import date_part, date_trunc


engine = sal.create_engine('postgresql://' +usrnm('vn')+':'+pwd('vn')+'@'+host()+':5432/'+db('vn'))
conn = engine.connect()

command = """select count("userID"), "activeDate" from user_active_events group by 2 """
dau = pd.DataFrame(engine.execute(command), columns = ['DAU', 'activeDate'])
dau["activeDate"]= pd.DatetimeIndex(dau["activeDate"])
avg_dau = dau
#avg_dau[["activeDate"]] = avg_dau[["activeDate"]].date()
#print("DAU")
#print(dau)
avg_dau["start_month"] = avg_dau["activeDate"].apply(date_trunc, args = ('month',))
avg_dau = avg_dau.groupby(["start_month"]).sum()
#avg_dau["daynum"] = avg_dau["start_month"].apply(date_part, args = ('eom',))
print("avg_DAU")
print(avg_dau.shape)
conn.close()
