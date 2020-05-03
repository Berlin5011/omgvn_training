import sqlalchemy as sal
from sqlalchemy import create_engine
import pandas as pd
from credential import usrnm, pwd, host, db

engine = sal.create_engine('postgresql://' +usrnm('vn')+':'+pwd('vn')+'@'+host()+':5432/'+db('vn'))
conn = engine.connect()

command = """select "userID", "activeDate" from user_active_events """

conn.close()