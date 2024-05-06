import pandas as pd
import datetime as dt

data = pd.read_csv(f'historical_spy_data.csv')

data.rename(columns={'date':'Timestamp', 'average': 'Price', 'volume':'Volume'}, inplace=True)
df = data[['Timestamp', 'Price', 'Volume']]

df['Interval'] = pd.to_datetime(df.Timestamp).dt.as_unit("s").dt.time
df['Day'] = pd.to_datetime(df.Timestamp).dt.date

d = ((6*60+30)//5) + 1
cutoff  = [(dt.datetime.min + (dt.timedelta(hours=9, minutes=30) + dt.timedelta(minutes=i*5))).time() for i in range(d)]

movement = []
chg = []
vpoc = []
total_volume = []
vpoc_tpoc_ratio = []
vpoc_close_ratio = []
tpoc_close_ratio = []
vpoc_price_hl_range = []
tpoc_price_hl_range = []
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

period = []
# detect multiple peaks
for d in df.Day.unique():
    for idx in range(1, len(cutoff)):
        window = df[(df.Day == d) & (df.Interval >= cutoff[idx-1]) & (df.Interval < cutoff[idx])]

        groupings = window.groupby("Price").Volume.sum().reset_index()
        poc_idx = groupings.Volume.argmax()
        
        vpoc.append(groupings.Volume.values[poc_idx])

        vpoc_price =  groupings.Price.values[poc_idx]
        tpoc = window.Price.mode().values[0]
        
        price_cv.append(round(window.Price.std() / window.Price.mean(),3))
        price_skew.append(round(window.Price.skew(),3))
        price_kurt.append(round(window.Price.kurt(),3))

        vol_price_cv.append(round(groupings.Volume.std() / groupings.Volume.mean(),3))
        vol_price_skew.append(round(groupings.Volume.skew(),3))
        vol_price_kurt.append(round(groupings.Volume.kurt(),3))

        high,low = groupings.Price.max(), groupings.Price.min()
        open_price, close_price = window.Price.values[0], window.Price.values[-1]
        pct_chg = round(100 * (close_price / open_price - 1),4)
        move = 1 if pct_chg >= 0 else 0
        
        chg.append(pct_chg)
        movement.append(move)
        
        total_volume.append(groupings.Volume.sum())

        vpoc_tpoc_ratio.append(abs(vpoc_price - tpoc) / ((high - low)))
        vpoc_close_ratio.append(abs(vpoc_price - close_price) / ((high - low)))
        tpoc_close_ratio.append(abs(tpoc - close_price) / ((high - low)))
        vpoc_price_hl_range.append((high - vpoc_price) / (high - low))
        tpoc_price_hl_range.append((high - tpoc) / (high - low))

        trapped_longs.append(1 if vpoc_price > close_price else 0)
        trapped_shorts.append(1 if vpoc_price < close_price else 0)
        
        # trapped_longs_at_extremes.append(1 if (vpoc_price >= close_price) & (vpoc_price >= open_price) else 0)
        # trapped_shorts_at_extremes.append(1 if (vpoc_price <= close_price) & (vpoc_price <= open_price) else 0)
        trapped_longs_at_extremes.append(1 if (vpoc_price >= close_price) & (vpoc_price >= open_price) & (tpoc >= close_price) & (tpoc >= open_price) else 0)
        trapped_shorts_at_extremes.append(1 if (vpoc_price <= close_price) & (vpoc_price <= open_price) & (tpoc <= close_price) & (tpoc <= open_price) else 0)

        period.append(str(d) + " " + str(cutoff[idx-1]))


df = pd.DataFrame({'period': period,'movement':movement, 'chg':chg, 'vpoc': vpoc, 'total_volume':total_volume, 
                   'vpoc_tpoc_ratio': vpoc_tpoc_ratio,
                   'vpoc_close_ratio':vpoc_close_ratio, 'tpoc_close_ratio':tpoc_close_ratio,
                   'vpoc_price_hl_range':vpoc_price_hl_range, 'tpoc_price_hl_range':tpoc_price_hl_range,
                   'price_cv': price_cv, 'price_skew': price_skew, 'price_kurt': price_kurt, 
                   'vol_price_cv':vol_price_cv, 'vol_price_skew':vol_price_skew, 'vol_price_kurt':vol_price_kurt,
                   'trapped_longs':trapped_longs, 'trapped_shorts':trapped_shorts,
                   'trapped_shorts_at_extremes':trapped_shorts_at_extremes, 'trapped_longs_at_extremes':trapped_longs_at_extremes
                    })

df.to_csv('spy_v2.csv',index=False)