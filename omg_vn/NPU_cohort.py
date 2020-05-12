import sqlalchemy as sal
from sqlalchemy import create_engine
from credential import usrnm, pwd, host, db
import pandas as pd
from datetime import timedelta
import time

start = time.time()
engine = create_engine('postgresql://'+usrnm('th')+':'+pwd('th')+'@'+host()+':5432/'+db('th'))
conn = engine.connect()
command = """with pre as(
                select 	"userID",
                        min("chargeDate") as "1stCharge"
                from dbg_add
                group by 1
            ),
            npu as (
                select	count(*) as "NPU",
                        "1stCharge"
                from pre
                group by 2
            )
            select	npu."NPU",
                    pre."userID",
                    pre."1stCharge",
                    dbg_add."chargeDate"
            from pre
            inner join dbg_add on dbg_add."userID" = pre."userID"
            inner join npu on npu."1stCharge" = pre."1stCharge"
		 """
dfu = pd.DataFrame(engine.execute(command), columns = ['NPU','userID','1stCharge','chargeDate'])
conn.close()
ans = dfu.groupby(by = ['NPU', '1stCharge', 'chargeDate']).count().reset_index()
ans.columns = ['NPU', '1stCharge', 'chargeDate', 'chargeUsers']
ans['sub'] = ans['chargeDate'] - ans['1stCharge']
print(ans.sort_values(by = ['1stCharge'], ascending = [True]))
