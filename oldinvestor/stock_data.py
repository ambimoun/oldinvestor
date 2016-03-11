import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker
import tushare as ts

Base = declarative_base()
Session = sessionmaker()


class BasicInformation(Base):
    __tablename__ = 'data_information'
    security_id = Column(String, primary_key=True)
    ticker = Column(String)
    type = Column(String)
    last_update_date = Column(String)

    def __repr__(self):
        return '<DataInformation(security_id={0}, last_update_date={1})>'.format(self.security_id, self.last_update_date)


class Stock(object):
    def __init__(self, ticker, engine):
        self.security_id = self.get_basic_information(ticker)
        #print('读取股票内部代码:{0}......'.format(arg_ticker))
        #self.secID = ts.Master().SecID(assetClass='E', ticker=arg_ticker).at[0, 'secID']
        self.engine = engine

    def get_basic_information(self, ticker):
        session = Session()
        print('从数据库中读取股票基本信息:{0}......'.format(ticker))
        query = session.query(BasicInformation).filter(BasicInformation.ticker == ticker).filter(
            BasicInformation.type == 'stock')
        try:
            basic_information = query.one()
            security_id = basic_information.security_id
        except sqlalchemy.orm.exc.NoResultFound:
            print('数据库中无该股票信息,将在网络读取......'.format(ticker))
            security_id = ts.Master().SecID(assetClass='E', ticker=ticker).at[0, 'secID']
            basic_information = BasicInformation(security_id=security_id, ticker=ticker, type='stock', last_update_date='20160304')
            session.add(basic_information)
        session.commit()
        return security_id

    def get_day_line(self):
        print('读取数据:日线数据......')
        day_line = ts.Market().MktEqud(secID=self.secID)
        if not day_line.empty:
            day_line = day_line.set_index('secID')
            day_line.to_sql('day_line', self.engine, if_exists='append')

    def get_balance_sheet(self):
        print('读取数据:合并资产负债表......')
        balance_sheet = ts.Fundamental().FdmtBS(secID=self.secID)
        if not balance_sheet.empty:
            balance_sheet = balance_sheet.drop_duplicates(subset=['endDate', 'reportType'], keep='last')
            balance_sheet = balance_sheet.set_index('secID')
            balance_sheet.to_sql('balance_sheet', self.engine, if_exists='append')

    def get_income_statement(self):
        print('读取数据:合并利润表......')
        income_statement = ts.Fundamental().FdmtIS(secID=self.secID)
        if not income_statement.empty:
            income_statement = income_statement.drop_duplicates(subset=['endDate', 'reportType'], keep='last')
            income_statement = income_statement.set_index('secID')
            income_statement.to_sql('income_statement', self.engine, if_exists='append')

    def get_cash_flow_statement(self):
        print('读取数据:合并现金流量表......')
        cash_flow_statement = ts.Fundamental().FdmtCF(secID=self.secID)
        if not cash_flow_statement.empty:
            cash_flow_statement = cash_flow_statement.drop_duplicates(subset=['endDate', 'reportType'], keep='last')
            cash_flow_statement = cash_flow_statement.set_index('secID')
            cash_flow_statement.to_sql('cash_flow_statement', self.engine, if_exists='append')

    def get_equity_allotment(self):
        print('读取数据:历史配股信息......')
        equity_allotment = ts.Equity().EquAllot(secID=self.secID)
        if not equity_allotment.empty:
            equity_allotment = equity_allotment.set_index('secID')
            equity_allotment.to_sql('equity_allotment', self.engine, if_exists='append')

    def get_equity_dividend(self):
        print('读取数据:历史分红信息......')
        equity_dividend = ts.Equity().EquDiv(secID=self.secID)
        if not equity_dividend.empty:
            equity_dividend = equity_dividend.set_index('secID')
            equity_dividend.to_sql('equity_dividend', self.engine, if_exists='append')

    # Unused
    def get_equity_share(self):
        print('读取数据:历史股本变动信息......')
        equity_share = ts.Equity().EquShare(secID=self.secID)
        if not equity_share.empty:
            equity_share = equity_share.set_index('secID')
            equity_share.to_sql('equity_share', self.engine, if_exists='append')

    # Unused
    def get_equity_splits(self):
        print('读取数据:历史拆股信息......')
        equity_splits = ts.Equity().EquSplits(secID=self.secID)
        if not equity_splits.empty:
            equity_splits = equity_splits.set_index('secID')
            equity_splits.to_sql('equity_share', self.engine, if_exists='append')

    def update_data(self):
        self.get_day_line()
        self.get_balance_sheet()
        self.get_income_statement()
        self.get_cash_flow_statement()
        self.get_equity_allotment()
        self.get_equity_dividend()
        self.update_data_information()


def set_token():
    if ts.get_token() is None:
        ts.set_token('03d8d816cd281b447e2809dfbac371a992620752da35392f5ea41c1be5e3f827')
        print('已设置token凭证码')


def init_database():
    engine = create_engine('sqlite:///data.db')
    Base.metadata.create_all(engine)
    Session.configure(bind=engine)
    return engine


def drop_table():
    import sqlite3
    conn = sqlite3.connect('data.db')
    cursor = conn.cursor()
    TABLE_NAME = ['day_line', 'balance_sheet', 'income_statement', 'cash_flow_statement', 'equity_allotment',
                  'equity_dividend', 'equity_share']
    for table in TABLE_NAME:
        try:
            cursor.execute('DROP TABLE {0}'.format(table))
        except:
            pass
    cursor.close()
    conn.commit()
    conn.close()


def main():
    drop_table()
    set_token()
    engine = init_database()
    s600886 = Stock('600886', engine)
    s600886.update_data()


if __name__ == '__main__':
    main()