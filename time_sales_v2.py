from ib_insync import *     #third party wrapper
import datetime as dt
import numpy as np
import pandas as pd
import pytz

ib = IB()
ib.connect('127.0.0.1', 7497, clientId=1)

contract = Stock(symbol='SPY',exchange='ARCA',currency='USD')

ib.qualifyContracts(contract)

start_day = dt.date(2024, 5, 13)
end_day = dt.date.today()

cur_day = start_day

while cur_day <= end_day:

    print(cur_day, flush=True)

    # Saturday: 5, Sunday: 6
    if cur_day.weekday() < 5:

        cur_time = dt.datetime(cur_day.year, cur_day.month, cur_day.day, 9, 30, 0)

        end_time = dt.datetime(cur_day.year, cur_day.month, cur_day.day, 16, 0, 0)

        df = pd.DataFrame()

        while cur_time < end_time:
            ticks = ib.reqHistoricalTicks(contract, cur_time.strftime("%Y%m%d %H:%M:%S US/Eastern"), "" , 1000, 'TRADES', useRth=True)
            prices = []
            volume = []
            timestamps = []

            if len(ticks) > 0:
                for tick in ticks:
                    prices.append(tick.price)
                    volume.append(tick.size)
                    timestamps.append(tick.time.astimezone(pytz.timezone("US/Eastern")).replace(tzinfo=None))
                
                df = pd.concat([df, pd.DataFrame({
                    "timestamp": timestamps,
                    "price": prices,
                    "volume": volume,
                })], copy=False, axis=0)

                cur_time = timestamps[-1] + dt.timedelta(seconds=1)
                print(timestamps[0],timestamps[-1], len(ticks), flush=True)
    
        df.to_csv(f'C:\\Users\\zeyad\\Desktop\\Intraday\\historical_spy_trades.csv',index=False,mode='a',header=False)

    cur_day = cur_day + dt.timedelta(days=1)
