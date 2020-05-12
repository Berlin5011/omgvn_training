import sqlalchemy as sal
from sqlalchemy import create_engine
from credential import usrnm, pwd, host, db
import pandas as pd
from datetime import timedelta
import time

start = time.time()
engine = create_engine('postgresql://'+usrnm('th')+':'+pwd('th')+'@'+host()+':5432/'+db('th'))
conn = engine.connect()
command = """select	date_trunc('day',"firstLoginDate") as "1stLog",
                    A."activeDate",
                    count(*)
            from user_profile
            inner join user_active_events A on A."userID" = user_profile."userID"
            where date_trunc('day',"firstLoginDate") >='2020-01-01'
            group by 1,2
		 """
dfu = pd.DataFrame(engine.execute(command), columns = ['1stLogin','activeDate','activeUsers'])
dfu['activeDate'] = pd.DatetimeIndex(dfu['activeDate'])
command_nru = """select count(*) as "NRU",
                        date_trunc('day', "firstLoginDate") as "1stLogin"
                from user_profile
                where date_trunc('day', "firstLoginDate") >= '2020-01-01'
                group by 2"""
nru = pd.DataFrame(engine.execute(command_nru), columns = ['NRU', '1stLogin']).sort_values(by = ['1stLogin'], ascending = [True])
conn.close()
ans = dfu.join(nru.set_index('1stLogin'), on = '1stLogin')
ans['sub'] = ans['activeDate'] - ans['1stLogin']
print(ans)
