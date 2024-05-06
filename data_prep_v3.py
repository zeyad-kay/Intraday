import pandas as pd
import datetime as dt

df = pd.read_csv(f'historical_spy_trades.csv')

df['Time'] = pd.to_datetime(df.timestamp).dt.as_unit("s").dt.time
df['Day'] = pd.to_datetime(df.timestamp).dt.date

d = ((6*60+30)//5) + 1
cutoff  = [(dt.datetime.min + (dt.timedelta(hours=9, minutes=30) + dt.timedelta(minutes=i*5))).time() for i in range(d)]

movement = []
chg = []

open_prices = []
close_prices = []
high_prices = []
low_prices = []

vpoc_prices = []
tpoc_prices = []

total_volume = []
vpoc = []

vpoc_tpoc_ratio = []
vpoc_open_ratio = []
tpoc_open_ratio = []
vpoc_high_ratio = []
tpoc_high_ratio = []
vpoc_low_ratio = []
tpoc_low_ratio = []
vpoc_close_ratio = []
tpoc_close_ratio = []

price_cv = []
price_skew = []
price_kurt = []

vol_price_cv = []
vol_price_skew =[]
vol_price_kurt = []

trapped_longs = []
trapped_shorts = []

trapped_longs_at_extremes = []
trapped_shorts_at_extremes = []

trapped_at_extremes_volume = []

period = []
# detect multiple peaks
for d in df.Day.unique():
    for idx in range(1, len(cutoff)):
        window = df[(df.Day == d) & (df.Time >= cutoff[idx-1]) & (df.Time < cutoff[idx])]

        groupings = window.groupby("price").volume.sum().reset_index()
        poc_idx = groupings.volume.argmax()
        
        vpoc.append(groupings.volume.values[poc_idx] / groupings.volume.sum())

        vpoc_price =  groupings.price.values[poc_idx]
        tpoc_price = window.price.mode().values[0]
        
        vpoc_prices.append(vpoc_price)
        tpoc_prices.append(tpoc_price)
        
        price_cv.append(round(window.price.std() / window.price.mean(),3))
        price_skew.append(round(window.price.skew(),3))
        price_kurt.append(round(window.price.kurt(),3))

        vol_price_cv.append(round(groupings.volume.std() / groupings.volume.mean(),3))
        vol_price_skew.append(round(groupings.volume.skew(),3))
        vol_price_kurt.append(round(groupings.volume.kurt(),3))

        high,low = groupings.price.max(), groupings.price.min()
        open_price, close_price = window.price.values[0], window.price.values[-1]
        pct_chg = round(100 * (close_price / open_price - 1),4)
        move = 1 if pct_chg >= 0 else 0
        
        chg.append(pct_chg)
        movement.append(move)

        open_prices.append(open_price)
        high_prices.append(high)
        low_prices.append(low)
        close_prices.append(close_price)
        
        total_volume.append(groupings.volume.sum())

        vpoc_tpoc_ratio.append(abs(vpoc_price - tpoc_price) / ((high - low)))
        
        vpoc_open_ratio.append(abs(vpoc_price - open_price) / ((high - low)))
        tpoc_open_ratio.append(abs(tpoc_price - open_price) / ((high - low)))

        vpoc_high_ratio.append((high - vpoc_price) / (high - low))
        tpoc_high_ratio.append((high - tpoc_price) / (high - low))

        vpoc_low_ratio.append((vpoc_price - low) / (high - low))
        tpoc_low_ratio.append((tpoc_price - low) / (high - low))

        vpoc_close_ratio.append(abs(vpoc_price - close_price) / ((high - low)))
        tpoc_close_ratio.append(abs(tpoc_price - close_price) / ((high - low)))
        

        trapped_longs.append(1 if vpoc_price > close_price else 0)
        trapped_shorts.append(1 if vpoc_price < close_price else 0)
        
        if (vpoc_price >= close_price) & (vpoc_price >= open_price) & (tpoc_price >= close_price) & (tpoc_price >= open_price):
            trapped_longs_at_extremes.append(1)
            trapped_shorts_at_extremes.append(0)
            trapped_at_extremes_volume.append(groupings[groupings.price >= max(open_price, close_price)].volume.sum() / groupings.volume.sum())
        
        elif (vpoc_price <= close_price) & (vpoc_price <= open_price) & (tpoc_price <= close_price) & (tpoc_price <= open_price):
            trapped_shorts_at_extremes.append(1)
            trapped_longs_at_extremes.append(0)
            trapped_at_extremes_volume.append(groupings[groupings.price <= min(open_price, close_price)].volume.sum() / groupings.volume.sum())

        else:
            trapped_shorts_at_extremes.append(0)
            trapped_longs_at_extremes.append(0)
            trapped_at_extremes_volume.append(0)

        period.append(str(d) + " " + str(cutoff[idx-1]))


df = pd.DataFrame({'period': period,'movement':movement, 'chg':chg, 'vpoc': vpoc, 'total_volume':total_volume, 
                   'vpoc_price': vpoc_prices, 'tpoc_price': tpoc_prices,
                   'open': open_prices, 'high': high_prices, 'low': low_prices, 'close': close_prices, 
                   'vpoc_tpoc_ratio': vpoc_tpoc_ratio,
                   'vpoc_open_ratio':vpoc_open_ratio, 'tpoc_open_ratio':tpoc_open_ratio,
                   'vpoc_high_ratio':vpoc_high_ratio, 'tpoc_high_ratio':tpoc_high_ratio,
                   'vpoc_low_ratio':vpoc_low_ratio, 'tpoc_low_ratio':tpoc_low_ratio,
                   'vpoc_close_ratio':vpoc_close_ratio, 'tpoc_close_ratio':tpoc_close_ratio,
                   'price_cv': price_cv, 'price_skew': price_skew, 'price_kurt': price_kurt, 
                   'vol_price_cv':vol_price_cv, 'vol_price_skew':vol_price_skew, 'vol_price_kurt':vol_price_kurt,
                   'trapped_longs':trapped_longs, 'trapped_shorts':trapped_shorts,
                   'trapped_shorts_at_extremes':trapped_shorts_at_extremes, 'trapped_longs_at_extremes':trapped_longs_at_extremes,
                   'trapped_at_extremes_volume':trapped_at_extremes_volume
                    })

df.to_csv('historical_spy_trades_agg.csv',index=False)