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

print("Daily active users (per day):")
print(dau)

avg_dau = dau
avg_dau["Month"] = avg_dau["activeDate"].apply(date_trunc, args = ('month',))
avg_dau = avg_dau.groupby(["Month"]).sum().reset_index()
avg_dau["DAU"] = avg_dau["DAU"] / avg_dau["Month"].apply(date_part, args = ('eom',))

print("Average daily active users (per month):")
print(avg_dau)

conn.close()
