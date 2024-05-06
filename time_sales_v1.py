from ib_insync import *     #third party wrapper
import datetime as dt
import pandas as pd


util.startLoop()

ib = IB()
ib.connect('127.0.0.1', 7497, clientId=1)

contract = Stock(symbol='SPY',exchange='ARCA',currency='USD')
#contract = Forex('USDJPY')
ib.qualifyContracts(contract)


# start = '20240328 15:30:00 US/Eastern'
# end = dt.datetime.now()
# ticks = ib.reqHistoricalTicks(contract, start, "", 1000, 'TRADES', useRth=True)


# timestamp = []
# price = []
# volume = []
# for tick in ticks:
#     timestamp.append(tick.time)
#     price.append(tick.price)
#     volume.append(tick.size)
# time_sales = pd.DataFrame({'Timestamp':timestamp,'Price':price, 'Volume':volume})


# print(time_sales)


# ############3
# ticker = ib.reqTickByTickData(contract, 'Last')
# ib.sleep(1)
# print(ticker)

# ib.cancelTickByTickData(contract, 'Last')
# ###############################

days = input("Enter number of days: ")

if days not in ['1','2','3']:
    print("days must be 1 or 2 or 3")
    exit(1)

bars = ib.reqHistoricalData(
        contract,
        endDateTime="",
        durationStr=f'{days} D',
        barSizeSetting='5 secs',
        whatToShow='TRADES',
        useRTH=True,
        formatDate=1)

df = util.df(bars)
print(df)
print(type(df))
df.to_csv(f'C:\\Users\\zeyad\\Desktop\\Intraday\\historical_spy_data.csv',index=False,mode='a',header=False)
