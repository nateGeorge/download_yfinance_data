import os

import wget
import yfinance as yf
from sqlalchemy import create_engine
import pandas_market_calendars as cal
import pandas as pd
from tqdm import tqdm


def chunks(l, n):
    """
    Yield successive n-sized chunks from a list l.
    https://stackoverflow.com/a/312464/4549682
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]


def download_stock_data(stocks=['QQQ', 'TQQQ', 'SQQQ'], db_file='stock_data.sqlite'):
    e = create_engine('sqlite:///stock_data.sqlite')
    con = e.connect()
    # get latest downloaded dates for all stocks in list
    today = pd.to_datetime(pd.datetime.utcnow()).tz_localize('UTC')
    ndq = cal.get_calendar('NASDAQ')
    sched = ndq.schedule(start_date='01-01-1900', end_date=today)
    end_date = today
    # if before the close today, use yesterday as last day
    if today < sched.iloc[-1]['market_close']:
        end_date = sched.iloc[-2]['market_close']

    start_dates = {}
    if e.dialect.has_table(e, 'data'):
        for s in stocks:
            res = con.execute('select max(Date) from data where ticker = "{}"'.format(s))
            date = pd.to_datetime(res.fetchone()[0])
            start_dates[s] = None
            if date is not None:
                start_dates[s] = date
    else:
        for s in stocks:
            start_dates[s] = None

    # TODO: group by start dates so we can multithread download
    for s in tqdm(stocks):
        start = start_dates[s]
        if start is None or start.date() < today.date() and start.date() != end_date.date():
            # start is non-inclusive, end is inclusive
            data = yf.download(s, auto_adjust=True, rounding=False, start=start, end=end_date)
            data['ticker'] = s
            data.to_sql(name='data', con=con, if_exists='append', index_label='date', method='multi')

    con.close()


def get_stocklists():
    link = 'ftp://ftp.nasdaqtrader.com/symboldirectory/{}.txt'
    for l in ['nasdaqlisted', 'otherlisted']:
        os.remove('{}.txt'.format(l))
        wget.download(link.format(l))

    ndq = pd.read_csv('nasdaqlisted.txt', sep='|')
    other = pd.read_csv('otherlisted.txt', sep='|')

    # last line has 'File createtd at...'
    drop_idx = ndq[ndq['Symbol'].str.contains('File')].index
    ndq.drop(drop_idx, inplace=True)
    drop_idx = other[other['ACT Symbol'].str.contains('File')].index
    other.drop(drop_idx, inplace=True)

    symbols = ndq.Symbol.to_list() + other['ACT Symbol'].to_list()

    return symbols


def download_stocklist():
    """
    downloads all stocks from nasdaq lists
    """
    symbols = get_stocklists()
    download_stocklist(symbols)


def load_data(ticker='QQQ'):
    e = create_engine('sqlite:///stock_data.sqlite')
    con = e.connect()
    current_data = pd.read_sql('select * from data where ticker = "{}";'.format(ticker), con)
    current_data['Date'] = pd.to_datetime(current_data['Date'])
    current_data.set_index('Date', inplace=True)
    current_data.sort_index(inplace=True)
    con.close()
    return current_data


if __name__=="__main__":
    download_stock_data()
