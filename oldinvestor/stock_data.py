from sqlalchemy import create_engine
import tushare as ts

df = ts.get_tick_data('600848', date='2014-12-22')
engine = create_engine("sqlite:///tusq.db")
df.to_sql('tick_data', engine)