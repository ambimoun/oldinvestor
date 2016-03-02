from sqlalchemy import create_engine
import tushare as ts


class Stock(object):
    def __init__(self, arg_ticker):
        master = ts.Master()
        if master.client.token is None:
            print('开始设置token凭证码,请稍候......')
            ts.set_token('03d8d816cd281b447e2809dfbac371a992620752da35392f5ea41c1be5e3f827')
            master = ts.Master()
        print('读取股票内部代码......')
        self.secID = master.SecID(assetClass='E', ticker=arg_ticker).at[0, 'secID']
        self.engine = create_engine('sqlite:///data.db')

    def get_day_line(self):
        print('读取数据----日线数据......')
        day_line = ts.Market().MktEqud(secID=self.secID)
        day_line = day_line.set_index('secID')
        day_line.to_sql('day_line', self.engine, if_exists='append')

    def get_income_statement(self):
        print('读取数据----合并利润表......')
        income_statement = ts.Fundamental().FdmtIS(secID=self.secID)
        income_statement = income_statement.drop_duplicates(subset=['endDate', 'reportType'], keep='last')
        income_statement = income_statement.set_index('secID')
        income_statement.to_sql('income_statement', self.engine, if_exists='append')

    def get_equity_allotment(self):
        print('读取数据----历史配股信息......')
        equity_allotment = ts.Equity().EquAllot(secID=self.secID)
        equity_allotment = equity_allotment.set_index('secID')
        equity_allotment.to_sql('equity_allotment', self.engine, if_exists='append')

    def update_data(self):
        self.get_day_line()
        self.get_income_statement()
        self.get_equity_allotment()


def drop_table():
    import sqlite3
    conn = sqlite3.connect('data.db')
    cursor = conn.cursor()
    try:
        cursor.execute('DROP TABLE {0}'.format('day_line'))
    except:
        pass
    try:
        cursor.execute('DROP TABLE {0}'.format('income_statement'))
    except:
        pass
    try:
        cursor.execute('DROP TABLE {0}'.format('equity_allotment'))
    except:
        pass
    cursor.close()
    conn.commit()
    conn.close()


def main():
    drop_table()
    s600886 = Stock('600886')
    s600886.update_data()


if __name__ == '__main__':
    main()