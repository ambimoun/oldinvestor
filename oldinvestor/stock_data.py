from sqlalchemy import create_engine
import tushare as ts


def get_day_data_old():
    day_data = ts.get_hist_data('600886')
    day_data['code'] = '600886'
    day_data = day_data.reset_index()
#    day_data = day_data[['code', 'date', 'open', 'high', 'close', 'low', 'volume']]
    day_data = day_data.set_index('code')
    engine = create_engine("sqlite:///data.db")
    day_data.to_sql('day_data_old', engine, if_exists='append')


def get_day_data():
    day_data = ts.Market().MktEqud(ticker='600886')
    day_data = day_data.set_index('ticker')
    engine = create_engine("sqlite:///data.db")
    day_data.to_sql('day_data', engine, if_exists='append')


def get_income_statement():
    income_statement = ts.Fundamental().FdmtIS(ticker='600886')
    income_statement = income_statement.set_index('secID')
    engine = create_engine('sqlite:///data.db')
    income_statement.to_sql('income_statement', engine, if_exists='append')

def main():
    ts.set_token('03d8d816cd281b447e2809dfbac371a992620752da35392f5ea41c1be5e3f827')
#    get_day_data_old()
#    get_day_data()
    get_income_statement()


if __name__ == '__main__':
    main()