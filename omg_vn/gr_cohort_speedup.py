import sqlalchemy as sal
from sqlalchemy import create_engine
import pandas as pd
from credential import usrnm, pwd, host, db
from datetime import timedelta
import time

start = time.time()
engine = create_engine('postgresql://'+usrnm('vn')+':'+pwd('vn')+'@'+host()+':5432/'+db('vn'))
conn = engine.connect()
command_u = """select 	dbg_add."userID",
		                date_trunc('day', user_profile."firstLoginDate"),
                        dbg_add."chargeDate",
                        sum(dbg_add."grossRev") as "gr"
                from dbg_add
                inner join user_profile on dbg_add."userID" = user_profile."userID"
                group by 1,2,3 """
dfu = pd.DataFrame(engine.execute(command_u), columns = ['userID', '1stLogin','chargeDate','gr'])
conn.close()
end = time.time()
print("Data query cost = "+str(end-start)+'s')
ans = pd.DataFrame(dfu["1stLogin"].unique(), columns = ["1stLogin"]).sort_values(by = "1stLogin", ascending = [True])
x = ["GR00", "GR01", "GR03", "GR07", "GR14", "GR21", "GR30"]
for i in x:
    diff = dfu[dfu["chargeDate"] - dfu["1stLogin"] <= timedelta(days = int(i[2:4]))].groupby(by = "1stLogin")["gr"].sum().reset_index()
    diff.columns = ['userID', i]
    ans = ans.join(diff.set_index('userID'), on = "1stLogin")
print(ans)
end2 = time.time()
print("Data processing cost = "+ str(end2-end)+'s')