import sqlalchemy as sal
from sqlalchemy import create_engine
import pandas as pd
from credential import usrnm, pwd, host, db
from datetime import timedelta
import time
#------------------------------------------------------------------
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
dfu = pd.DataFrame(engine.execute(command_u), columns = ['userID', '1stLogin','chargeDate','gr']).sort_values(by = ['1stLogin', 'chargeDate'], ascending = [True, True])
conn.close()
end = time.time()
print("Data query cost = "+str(end-start)+'s')
dfu['sub'] = dfu['chargeDate'] - dfu['1stLogin']
nru = dfu.groupby(by = ['1stLogin'])['userID'].count()
ans = dfu.groupby(by = ['1stLogin', 'sub'])['gr'].sum().reset_index()
print(ans[ans['sub']<= timedelta(days = 30)].groupby(by = ['1stLogin']).sum())
end2 = time.time()
print(end2 - end)