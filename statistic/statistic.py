# -*- coding: utf-8 -*-
import pandas as pd
import datetime

delivery_order = pd.read_excel('ex1.xlsx')
delivery_order[u'成交日期'] = delivery_order[u'成交日期'].astype(str)
delivery_order[u'证券代码'] = delivery_order[u'证券代码'].astype(str).str.zfill(6)
delivery_order_day_day_rich = delivery_order[delivery_order[u'证券代码']=='940018']
day_day_rich_list = []
pending_data = []
for row in delivery_order_day_day_rich[[u'成交日期', u'发生金额']].iterrows():
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