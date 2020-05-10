import sqlalchemy as sal
from sqlalchemy import create_engine
import pandas as pd
from credential import usrnm, pwd, host, db
from datetime import timedelta
import time

start = time.time()
engine = create_engine('postgresql://'+usrnm('vn')+':'+pwd('vn')+'@'+host()+':5432/'+db('vn'))
conn = engine.connect()
command_u = """with presum as(
                    select	dbg_add."userID",
                            dbg_add."chargeDate",
                            date_trunc('day',user_profile."firstLoginDate") as "firstLoginDate",
                            case
                            when dbg_add."chargeDate" = date_trunc('day',user_profile."firstLoginDate") then dbg_add."grossRev"
                            else 0
                            end as "GrossRev00",
                            case
                            when dbg_add."chargeDate" <= date_trunc('day',user_profile."firstLoginDate") + interval '1 day' then dbg_add."grossRev"
                            else 0
                            end as "GrossRev01",
                            case
                            when dbg_add."chargeDate" <= date_trunc('day',user_profile."firstLoginDate") + interval '3 days' then dbg_add."grossRev"
                            else 0
                            end as "GrossRev03",
                            case
                            when dbg_add."chargeDate" <= date_trunc('day',user_profile."firstLoginDate") + interval '7 days' then dbg_add."grossRev"
                            else 0
                            end as "GrossRev07",
                            case
                            when dbg_add."chargeDate" <= date_trunc('day',user_profile."firstLoginDate") + interval '14 days' then dbg_add."grossRev"
                            else 0
                            end as "GrossRev14",
                            case
                            when dbg_add."chargeDate" <= date_trunc('day',user_profile."firstLoginDate") + interval '21 days' then dbg_add."grossRev"
                            else 0
                            end as "GrossRev21",
                            case
                            when dbg_add."chargeDate" <= date_trunc('day',user_profile."firstLoginDate") + interval '30 days' then dbg_add."grossRev"
                            else 0
                            end as "GrossRev30"
                    from dbg_add
                    inner join user_profile on dbg_add."userID" = user_profile."userID"
                    order by 3
                )
                select	"firstLoginDate",
                        sum("GrossRev00") as "GrossRev00",
                        sum("GrossRev01") as "GrossRev01",
                        sum("GrossRev03") as "GrossRev03",
                        sum("GrossRev07") as "GrossRev07",
                        sum("GrossRev14") as "GrossRev14",
                        sum("GrossRev21") as "GrossRev21",
                        sum("GrossRev30") as "GrossRev30"
                from presum
                group by "firstLoginDate"
                order by 1 """
dfu = pd.DataFrame(engine.execute(command_u), columns = ['1stLogin', 'GR00','GR01','GR03','GR07','GR14','GR21','GR30'])
print(dfu)
end = time.time()
print('cost = '+str(end-start))