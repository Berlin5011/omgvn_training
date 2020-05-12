import sqlalchemy as sal
from sqlalchemy import create_engine
import pandas as pd
from credential import usrnm, pwd, host, db
from datetime import timedelta
import time
#------------------------------------------------------------------
start = time.time()
engine = create_engine('postgresql://'+usrnm('th')+':'+pwd('th')+'@'+host()+':5432/'+db('th'))
conn = engine.connect()
command_u = """select 	dbg_add."userID",
		        date_trunc('day', user_profile."firstLoginDate"),
                        dbg_add."chargeDate",
                        sum(dbg_add."grossRev") as "gr"
                from dbg_add
                inner join user_profile on dbg_add."userID" = user_profile."userID"
                group by 1,2,3 """
dfu = pd.DataFrame(engine.execute(command_u), columns = ['userID', '1stLogin','chargeDate','gr']).sort_values(by = ['1stLogin', 'chargeDate'], ascending = [True, True])
command_nru = """select count(*) as "NRU",
                        date_trunc('day', "firstLoginDate") as "1stLogin"
                from user_profile
                group by 2"""
nru = pd.DataFrame(engine.execute(command_nru), columns = ['NRU', '1stLogin']).sort_values(by = ['1stLogin'], ascending = [True])
conn.close()
end = time.time()
print("Data query cost = "+str(end-start)+'s')

dfu['sub'] = dfu['chargeDate'] - dfu['1stLogin']
gr = dfu.groupby(by = ['1stLogin', 'sub'])['gr'].sum().reset_index()
ans = gr.join(nru.set_index('1stLogin'), on = '1stLogin')
ans['LTV'] = ans['gr']/ans['NRU']
end2 = time.time()
print(ans)
print(end2 - end)