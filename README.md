# download_yfinance_data
Downloads Yahoo Finance data with yfinance in Python.

# Usage

It's best to customize the list of stocks you want to download.  Change the list at the bottom of download_stocks.py.  Then run the file: `python download_stocks.py`.
You can also download the entire stocklist from NASDAQ, which should take around 15 minutes to download the data, then another few minutes to store in a sqlite db.   To download all stocks, you can do:

`ipython`

```python
import download_stocks as ds
ds.download_stocklist()
```

# List of stocks
Retrieved from here: ftp://ftp.nasdaqtrader.com/symboldirectory


# Notes
Currently, yfinance is correcting the OHL for dividends by using the same ratio as for the close.  I think this is incorrect, and it should be using the dividends to subtract that amount from the days prior to the dividend.

# To do
- Add other storage mechanisms (arctic db which uses MongoDB, postgresql, etc)
- Error checking/testing
