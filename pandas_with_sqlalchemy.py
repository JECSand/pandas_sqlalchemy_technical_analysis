# Connor Sanders
# Sample Python Data Analytics Script and Storage to the Cloud
# 10/26/2017
# Developed and Tested using Python 3.5 on Debian 9

# Before running the script, make sure to set up a PostgreSQL Database and configure the code from lines 50-54

# Package installer function to handle missing packages
def install(package):
    print(package + ' package for Python not found, pip installing now....')
    pip.main(['install', package])
    print(package + ' package has been successfully installed for Python\n Continuing Process...')

try:
    from yahoofinancials import YahooFinancials
except:
    install('yahoofinancials')
    from yahoofinancials import YahooFinancials

try:
    import pandas as pd
    import numpy as np
except:
    install('pandas')
    import pandas as pd
    import numpy as np

try:
    import sqlalchemy
    from sqlalchemy import *
    from sqlalchemy import create_engine
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy import Column, Integer, String, Numeric, DateTime
    from sqlalchemy.orm import sessionmaker
except:
    install('sqlalchemy')
    import sqlalchemy
    from sqlalchemy import *
    from sqlalchemy import create_engine
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy import Column, Integer, String, Numeric, DateTime
    from sqlalchemy.orm import sessionmaker


'''
Script Setup Variables
'''

# Enter your PostgreSQL Database Information here
user_name = 'username'
password = 'password'
host = 'host/end-point'
port = 'port'
database = 'database_name'
db_conn_str = 'postgresql://' + user_name + ':' + password + '@' + host + ':' + port + '/' + database

# Connect to the AWS PostgreSQL Database
engine = create_engine(db_conn_str)
Base = declarative_base()
conn = engine.connect()

# Select Tickers and stock history dates
ticker = 'AAPL'
ticker2 = 'MSFT'
ticker3 = 'INTC'
index = '^NDX'
freq = 'daily'
start_date = '2012-10-01'
end_date = '2017-10-01'

# Model Tables and create if they do not exist
metadata = MetaData()
rsiTable = Table('rsiTable', metadata,
    Column('id', Integer, primary_key=True),
    Column('Date', DateTime),
    Column('NDX', Numeric),
    Column('AAPL', Numeric),
    Column('MSFT',Numeric),
    Column('INTL', Numeric)
)

betaTable = Table('betaTable', metadata,
    Column('id', Integer, primary_key=True),
    Column('Date', DateTime),
    Column('AAPL', Numeric),
    Column('MSFT', Numeric),
    Column('INTL', Numeric)
)
rsiTable.create(engine)
betaTable.create(engine)


#Declaration of the RSI Table class in order to write RSI Data into the database.
class RSITable(Base):
    __tablename__ = 'rsiTable'
    id = Column(Integer, primary_key=True)
    Date = Column(DateTime())
    NDX = Column(Numeric())
    AAPL = Column(Numeric())
    MSFT = Column(Numeric())
    INTL = Column(Numeric())
    def __repr__(self):
        return "(id='%s', Date='%s', NDX='%s', AAPL='%s, MSFT='%s', INTL='%s')" % \
               (self.id, self.Date, self.NDX, self.AAPL, self.MSFT, self.INTL)


# Declaration of the Beta Table class in order to write Beta Data into the database.
class BetaTable(Base):
    __tablename__ = 'betaTable'
    id = Column(Integer, primary_key=True)
    Date = Column(DateTime())
    AAPL = Column(Numeric())
    MSFT = Column(Numeric())
    INTL = Column(Numeric())
    def __repr__(self):
        return "(id='%s', Date='%s', AAPL='%s, MSFT='%s', INTL='%s')" % \
                (self.id, self.Date, self.AAPL, self.MSFT, self.INTL)


# Function to clean data extracts
def clean_stock_data(stock_data_list):
    new_list = []
    for rec in stock_data_list:
        if 'type' not in rec.keys():
            new_list.append(rec)
    return new_list

# Construct yahoo financials objects for data extraction
aapl_financials = YahooFinancials(ticker)
mfst_financials = YahooFinancials(ticker2)
intl_financials = YahooFinancials(ticker3)
index_financials = YahooFinancials(index)

# Clean returned stock history data and remove dividend events from price history
daily_aapl_data = clean_stock_data(aapl_financials
                                     .get_historical_stock_data(start_date, end_date, freq)[ticker]['prices'])
daily_msft_data = clean_stock_data(mfst_financials
                                     .get_historical_stock_data(start_date, end_date, freq)[ticker2]['prices'])
daily_intl_data = clean_stock_data(intl_financials
                                     .get_historical_stock_data(start_date, end_date, freq)[ticker3]['prices'])
daily_index_data = index_financials.get_historical_stock_data(start_date, end_date, freq)[index]['prices']
stock_hist_data_list = [{'NDX': daily_index_data}, {'AAPL': daily_aapl_data}, {'MSFT': daily_msft_data},
                        {'INTL': daily_intl_data}]


'''
Stock Beta Algorithm
'''

# Function to construct data frame based on a stock and it's market index
def build_beta_data_frame(data_list1, data_list2, data_list3, data_list4):
    data_dict = {}
    i = 0
    for list_item in data_list2:
        if 'type' not in list_item.keys():
            data_dict.update({list_item['formatted_date']: {'NDX': data_list1[i]['close'], 'AAPL': list_item['close'],
                                                            'MSFT': data_list3[i]['close'],
                                                            'INTL': data_list4[i]['close']}})
            i += 1
    tseries = pd.to_datetime(list(data_dict.keys()))
    df = pd.DataFrame(data=list(data_dict.values()), index=tseries,
                      columns=['NDX', 'AAPL', 'MSFT', 'INTL']).sort_index()
    return df


# Function to create the groupby object ready for beta analysis
def roll_function(df, w):
    roll_array = np.dstack([df.values[i:i+w, :] for i in range(len(df.index) - w + 1)]).T
    panel = pd.Panel(roll_array,
                     items=df.index[w-1:],
                     major_axis=df.columns,
                     minor_axis=pd.Index(range(w), name='roll'))
    return panel.to_frame().unstack().T.groupby(level=0)


# Function to calculate Stock Beta
def calc_beta(df):
    X = df.values[:, [0]]
    X = np.concatenate([np.ones_like(X), X], axis=1)
    b = np.linalg.pinv(X.T.dot(X)).dot(X.T).dot(df.values[:, 1:])
    return pd.Series(b[1], df.columns[1:], name='Beta')


# Function to kick off stock beta calculation process
def get_stock_beta_df():
    df = build_beta_data_frame(daily_index_data, daily_aapl_data, daily_msft_data, daily_intl_data)
    roll_df = roll_function(df, 260)
    beta_vals = roll_df.apply(calc_beta)
    return beta_vals


'''
Stock RSI Algorithm
'''

# Function to build stock price series
def build_price_series(data_list):
    series_data_list = []
    series_date_list = []
    for list_item in data_list:
        series_data_list.append(list_item['close'])
        series_date_list.append(list_item['formatted_date'])
    tseries = pd.to_datetime(series_date_list)
    ds = pd.Series(series_data_list, index=tseries).sort_index(ascending=True)
    return ds


# Function to calculate stock's RSI
def calc_stock_rsi(prices, n=14):
    gain = (prices-prices.shift(1)).fillna(0)
    def calc_rsi(price):
        avg_gain = price[price>0].sum()/n
        avg_loss = -price[price<0].sum()/n
        rs = avg_gain/avg_loss
        return 100 - 100/(1+rs)
    return pd.rolling_apply(gain,n,calc_rsi)


# Function to kick off stock rsi calculation process
def get_stock_rsi_df():
    ds_list = []
    column_list = []
    for li_dict in stock_hist_data_list:
        for k, v in li_dict.items():
            ds = build_price_series(v)
            rsi_vals = calc_stock_rsi(ds)
            ds_list.append(rsi_vals)
            column_list.append(k)
    df = pd.concat(ds_list, axis=1)
    df.columns = column_list
    return df


# Main Function of the Script
def main():
    rsi_df = get_stock_rsi_df()
    beta_df = get_stock_beta_df()
    rsi_df.reset_index(inplace=True)
    rsi_df.rename(columns={'index': 'Date'}, inplace=True)
    beta_df.reset_index(inplace=True)
    beta_df.rename(columns={'index': 'Date'}, inplace=True)
    print('Beta Dataframe: ')
    print(beta_df)
    print('\nRSI Dataframe')
    print(rsi_df)
    rsiTableToWriteTo = 'rsiTable'
    betaTableToWriteTo = 'betaTable'
    rsi_df.to_sql(name=rsiTableToWriteTo, con=engine, if_exists='append', index=False)
    beta_df.to_sql(name=betaTableToWriteTo, con=engine, if_exists='append', index=False)
    Session = sessionmaker(bind=engine)
    session = Session()
    session.commit()
    session.close()
    print('Process Complete! New Beta and RSI Data is now available in your PostgreSQL Database!')

main()