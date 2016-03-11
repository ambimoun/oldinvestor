# -*- coding: utf-8 -*-
from collections import defaultdict
import pandas as pd
import datetime

delivery_order = pd.read_excel('ex1.xlsx')
delivery_order = delivery_order.set_index(u'成交日期')
delivery_order.index = delivery_order.index.astype(str)
delivery_order[u'证券代码'] = delivery_order[u'证券代码'].astype(str).str.zfill(6)


position_dict = defaultdict(dict)
richer_daily_in_date = None
for row in delivery_order.iterrows():
    date = datetime.datetime.strptime(row[0], '%Y%m%d').date()
    weekday = date.weekday() + 1
    ticker_symbol = row[1][u'证券代码']
    summary = row[1][u'摘要']
    amount = row[1][u'发生金额']
    if ticker_symbol == '940018':
        if summary == u'基金资金拨出':
            position_dict[date]['weekday'] = weekday
            position_dict[date]['richer_daily'] = -amount
        else:
            if richer_daily_in_date:
                balance = 0
                while richer_daily_in_date < date:
                    if richer_daily_in_date in position_dict:
                        balance -= position_dict[richer_daily_in_date]['richer_daily']
                    richer_daily_in_date += datetime.timedelta(days=1)
                balance += amount
                if balance != 0:
                    position_dict[date]['richer_daily_dividend'] = balance
            else:
                richer_daily_in_date = date
position = pd.DataFrame(position_dict).fillna(0).T
position['sum_total'] = position['richer_daily'] + position['richer_daily_dividend']
print(position)
"""
delivery_order_day_day_rich = delivery_order[delivery_order[u'证券代码']=='940018']
day_day_rich_list = []
pending_data = []

for row in delivery_order_day_day_rich[[u'成交日期', u'发生金额']].iterrows():
    #print(row[u'成交日期'])
    date = datetime.datetime.strptime(row[1][0], '%Y%m%d')
    amount = row[1][1]
    weekday = date.weekday() + 1
    if pending_data:
        if date != pending_data[0]:
            day_day_rich_list.append(pending_data)
            if amount < 0:
                pending_data = [date, weekday, -amount, 0]
            else:
                pending_data = [date, weekday, 0, amount]
        else:
            if amount < 0:
                pending_data[2] = -amount
            else:
                pending_data[3] = amount
    else:
        if amount < 0:
            pending_data = [date, weekday, -amount, 0]
        else:
            pending_data = [date, weekday, 0, amount]
day_day_rich_list.append(pending_data)
day_day_rich = pd.DataFrame(day_day_rich_list, columns=['日期', '星期', '存入', '取出']).set_index('日期')
print(day_day_rich)
#print(datetime.datetime.strptime(delivery_order[u'成交日期'][0], '%Y%m%d').date()+datetime.timedelta(days=1))
"""