import os

import wget
import yfinance as yf
from sqlalchemy import create_engine
import pandas_market_calendars as cal
import pandas as pd
from tqdm import tqdm
from arctic import Arctic


def chunks(l, n):
    """
    Yield successive n-sized chunks from a list l.
    https://stackoverflow.com/a/312464/4549682
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]


class downloader():
    def __init__(self, stocks=['QQQ', 'TQQQ', 'SQQQ'], db='sqlite', storage_dir=None, db_file='stock_data.sqlite'):
        self.db = db
        if storage_dir is None:
            home_dir = os.path.expanduser("~")
            self.storage_dir = os.path.join(home_dir, '.yfinance_data')
            if not os.path.exists(self.storage_dir):
                os.makedirs(self.storage_dir)
        else:
            self.storage_dir = storage_dir

        if db == 'sqlite':
            self.db_file = db_file
            # 4 slashes for absolute path: https://docs.sqlalchemy.org/en/13/core/engines.html#sqlite
            self.e = create_engine('sqlite:///{}/{}'.format(self.storage_dir, self.db_file))
            self.con = self.e.connect()
        else:
            self.e = None
            self.con = None

        if db == 'arctic':
            self.store = Arctic('localhost')
            self.store.initialize_library('yfinance_stockdata')
            self.library = self.store['yfinance_stockdata']

        self.stocks = stocks


    def get_stock_groups(self):
        """
        gets latest downloaded dates for all stocks in list;
        groups stocks by start date for multithreaded download
        """
        print('getting start dates for existing data...')
        start_dates = {}

        if self.db == 'sqlite':
            if self.e.dialect.has_table(self.e, 'data'):
                for s in tqdm(self.stocks):
                    res = self.con.execute('select max(Date) from data where ticker = "{}"'.format(s))
                    date = pd.to_datetime(res.fetchone()[0])
                    start_dates[s] = pd.NaT
                    if date is not None:
                        start_dates[s] = date
            else:
                for s in self.stocks:
                    start_dates[s] = pd.NaT

        elif self.db == 'arctic':
            symbols_in_lib = set(self.library.list_symbols())
            for s in self.stocks:
                if s in symbols_in_lib:
                    item = self.library.read(s)
                    df = item.data
                    if df.shape[0] == 0:
                        start_dates[s] = pd.NaT
                    else:
                        start_dates[s] = df.index.max()
                else:
                    start_dates[s] = pd.NaT

        # group by start dates so we can multithread download
        start_date_df = pd.DataFrame(data={'ticker': list(start_dates.keys()), 'start_date': list(start_dates.values())})
        unique_dates = start_date_df['start_date'].dt.date.unique()
        groups = []
        for udate in unique_dates:
            if pd.isnull(udate):
                tickers = start_date_df[pd.isnull(start_date_df['start_date'])]['ticker'].tolist()
            else:
                tickers = start_date_df[start_date_df['start_date'] == pd.Timestamp(udate)]['ticker'].tolist()

            groups.append(tickers)

        return unique_dates, groups


    def download_stock_data(self):
        unique_dates, groups = self.get_stock_groups()

        today = pd.to_datetime(pd.datetime.utcnow()).tz_localize('UTC')
        ndq = cal.get_calendar('NASDAQ')
        sched = ndq.schedule(start_date='01-01-1900', end_date=today)
        end_date = today
        # if before the close today, use yesterday as last day
        if today < sched.iloc[-1]['market_close']:
            end_date = sched.iloc[-2]['market_close']

        for start, grp in zip(unique_dates, groups):
            if pd.isnull(start) or start < today.date() and start != end_date.date():
                # start is non-inclusive, end is inclusive
                if pd.isnull(start):
                    start = None
                data = yf.download(grp, auto_adjust=True, rounding=False, start=start, end=end_date)
                dfs = []
                for s in grp:
                    try:
                        df = data.xs(s, level=1, axis=1).copy()
                    except AttributeError:
                        df = data.copy()

                    if 'Adj Close' in df.columns:
                        df.drop(columns='Adj Close', inplace=True)

                    # on error some dfs have 0 rows, but adj close column...ignore these
                    if df.shape[0] == 0:
                        continue

                    df.dropna(inplace=True)
                    if self.db in ['sqlite']:
                        df['ticker'] = s
                        dfs.append(df)
                    elif self.db == 'arctic':
                        self.library.write(s, df)

                # store data in sql
                if self.db in ['sqlite']:
                    print('writing data to sql db...')
                    # write to db in chunks to avoid crashing on full memory
                    # only seem to be able to write about 30k rows at once with sqlite3
                    for c in tqdm(chunks(dfs, 100)):
                        full_df = pd.concat(dfs, axis=0)
                        full_df.to_sql(name='data', con=self.con, if_exists='append', index_label='date', method='multi', chunksize=30000)


            else:
                print('Stock group up to date, not downloading.')


    def get_stocklists(self):
        link = 'ftp://ftp.nasdaqtrader.com/symboldirectory/{}.txt'
        for l in ['nasdaqlisted', 'otherlisted']:
            filename = '{}.txt'.format(l)
            if os.path.exists(filename):
                os.remove(filename)

            wget.download(link.format(l))

        ndq = pd.read_csv('nasdaqlisted.txt', sep='|')
        other = pd.read_csv('otherlisted.txt', sep='|')

        # last line has 'File createtd at...'
        drop_idx = ndq[ndq['Symbol'].str.contains('File')].index
        ndq.drop(drop_idx, inplace=True)
        drop_idx = other[other['ACT Symbol'].str.contains('File')].index
        other.drop(drop_idx, inplace=True)
        # ignore preferred stocks ($), warrants (.W), and units (.U)
        for smb in ['\.W', '\.U', '$']:
            drop_idx = other[other['ACT Symbol'].str.contains(smb)].index
            other.drop(drop_idx, inplace=True)

        symbols = ndq.Symbol.to_list() + other['ACT Symbol'].to_list()

        return symbols


    def download_stocklist(self):
        """
        downloads all stocks from nasdaq lists
        """
        symbols = self.get_stocklists()
        self.stocks = symbols
        self.download_stock_data()


class loader(downloader):
    def __init__(self):
        super().__init__(self)

    def load_data(self, ticker='QQQ'):
        if self.db in ['sqlite']:
            current_data = pd.read_sql('select * from data where ticker = "{}";'.format(ticker), self.con)
            current_data['date'] = pd.to_datetime(current_data['date'])
            current_data.set_index('date', inplace=True)
            current_data.sort_index(inplace=True)
        elif self.db == 'arctic':
            item = self.library.read(ticker)
            current_data = item.data

        return current_data

    def load_all_data(self):
        pass
