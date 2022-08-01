#!/usr/bin/python3

# TODO:
#
#  - Add arguments for file, ticker, and dates.
#  - Generate a new price instead of cloning an existing one.

import datetime
from fractions import Fraction

import pandas_datareader.data as web
from gnucash import GncPrice, Session


def main():

    # Download data from yahoo
    #df = web.DataReader('GE', 'yahoo', start='2019-09-10', end='2019-10-09')
    import pandas as pd
    df = pd.read_csv('data.csv', index_col=0)

    FILE = "test2.gnucash"
    uri = "file://"+FILE
    # TODO: Open the session using a context manager.  My current version of
    # GnuCash (3.8) doesn't support this, but newer versions do.
    # with Session(uri) as session:
    #     pass

    session = Session(uri)#, ignore_lock=False, is_new=False, force_new=True)
    try:
        # Get a list of the existing prices for the commondity.
        book = session.book
        price_db = book.get_price_db()
        print(price_db)
        commodity_table = book.get_table()
        commodity = commodity_table.lookup('NYSE', 'GE')
        currency = commodity_table.lookup('CURRENCY', 'USD')
        price_list = price_db.get_prices(commodity, currency)
        if len(price_list)<1:
            print ('Need at least one database entry to clone ...')
            return

        # We will use the first price as a template for new prices.
        price_0 = price_list[0]

        # Add the new prices to the list.
        for date_str in df.index:
            close = Fraction.from_float( df.at[date_str, 'Close']).limit_denominator(1000000)
            print(f'Adding {date_str} {close}')

            price_new = price_0.clone(book)
            price_new = GncPrice(instance=price_new)
            date = datetime.datetime(*(int(v) for v in date_str.split('-')))
            price_new.set_time64(date)

            close_new = price_new.get_value()
            close_new.num = close.numerator
            close_new.denom = close.denominator
            price_new.set_value(close_new)
            price_db.add_price(price_new)
    except Exception as e:
        raise(e)
    else:
        # Save is successful
        session.save()
    finally:
        # Clean up
        session.end()
        session.destroy()


if __name__ == "__main__":
    main()
