from pybit.unified_trading import HTTP
from pybit import exceptions
import pandas as pd
import math
from time import sleep
import datetime

# Function to update EMA
def update_ema(current, period, previous):
    multiplier = 2 / (period + 1)
    ema = current * multiplier + previous * (1 - multiplier)
    return ema

def get_95(df):
    mean = df.mean()["histogram"]
    std = df.std()["histogram"]
    return 	3.1*std + mean

# Initialize the Bybit API client
session = HTTP(
    testnet = False,
    api_key = '0LVZwXtRF8uxbLpv5d', 
    api_secret ='sueHTB8wKAKXF60Fv7D0urf8aBUiMMN60HHX',
)

symbols = session.get_tickers(
    category="linear",
)["result"]["list"]

volumns = [float(inner_array["volume24h"]) for inner_array in symbols if inner_array]
dfS = pd.DataFrame(
    {
        "symbols":[inner_array["symbol"] for inner_array in symbols if inner_array],
        "volumns":volumns
    }
)

mean = dfS["volumns"].mean()
std = dfS["volumns"].std()
quantile = 2.806*std + mean
dfS = dfS[dfS["volumns"] <= quantile]


mean = dfS["volumns"].mean()
std = dfS["volumns"].std()
quantile = 1.96*std + mean
dfS = dfS[dfS["volumns"] >= quantile]

df = pd.DataFrame()
histogram_5 = pd.DataFrame()
first = True
for symbol in dfS["symbols"]:
    # Extract data for each candles
    candles = session.get_kline(
        category="linear",
        symbol= symbol,
        interval='1',
        limit=1000,
    )["result"]["list"]
    
    # Extract current/close price
    prices = [float(inner_array[4]) for inner_array in candles]
    times = [float(inner_array[0]) for inner_array in candles]
    volumns = [float(inner_array[5]) for inner_array in candles]
    # Reverse the lists for chronological order
    prices.reverse()
    times.reverse()
    volumns.reverse()

    temp = pd.DataFrame({
        "prices":prices,
        "volumns":volumns
    },index=[[symbol]*len(times), times])

    temp.index.names = ["symbol","time"]
    # Calculating MACD
    temp["ema_12"] = temp["prices"].ewm(span=12).mean()
    temp["ema_26"] = temp["prices"].ewm(span=26).mean()
    temp["macd"] = temp["ema_12"] - temp["ema_26"]
    temp["signal"] = temp["macd"].ewm(span=9).mean()
    temp["histogram"] = temp["macd"] - temp["signal"]
    
    temp_h5 = temp[temp.index.get_level_values(1) % 300000 == 240000]

    if times[-1] % 300000 != 240000:
    # Add the last row to histogram_5
        new_hist =  pd.DataFrame([temp.iloc[-1]])
        temp_h5 = pd.concat([temp_h5,new_hist])
    
    df = pd.concat([df,temp])
    histogram_5 = pd.concat([histogram_5,temp_h5])
    histogram_5.index.names = ["symbol","time"]


    # Set leverage
    try:
        print(session.set_leverage(
            category="linear",
            symbol=symbol,
            buyLeverage="15",
            sellLeverage="15",
        ))
    except Exception as e:
        print(e)

# Initialize variables
key = True

# Set initial count
count = 3001
while True:
    for index,series in dfS.iterrows():
        try:
            symbol = series["symbols"]
            # Get real-time market price data
            orders = session.get_open_orders(
                category="linear",
                symbol=symbol,
                openOnly=0,
                limit=1,
            )["result"]["list"]
            
            candle = session.get_kline(
                category="linear",
                symbol=symbol,
                interval='1',
                limit=1,
            )["result"]["list"][0]

            price = float(candle[4])
            time = float(candle[0])
            volumn = float(candle[5])
            
            key = False if len(orders) >= 1 or count < 1501 else True
            temp = df.loc[symbol]
            temp_h5 = histogram_5.loc[symbol]

        # Check for duplicate time and remove corresponding elements from lists
            
            # Concatenate the new DataFrame with the original DataFrame
            # Update EMA values
            ema_12 = update_ema(price, 12,  temp.iloc[-1]["ema_12"])
            ema_26 = update_ema(price, 26,  temp.iloc[-1]["ema_26"])
            macd = ema_12 - ema_26
            signal = update_ema(macd, 9,  temp.iloc[-1]["signal"])
            histogram = macd - signal
            new_row = (price, None, ema_12, ema_26, macd, signal, histogram)
        
            # Create a new DataFrame for the new row
            df.loc[(symbol,time),:] = new_row
            df = df.sort_index()
            
            histogram_5.loc[(symbol,time),:] = new_row
            histogram_5 = histogram_5.sort_index()

            # Calculate the trigger point
            trigger = get_95(temp_h5)
            # Place sell order if MACD crosses above a threshold
            if histogram >= trigger and key:
                order = session.place_order(
                    category="linear",
                    symbol=symbol,
                    side="Sell",
                    orderType="Market",
                    qty= f"{int(15/price)}",
                    price= f"{price}",
                    isLeverage=1,
                    orderFilter="tpslOrder",
                )
                print(order)
                stop_price = float(session.get_positions(
                    category="inverse",
                    symbol=symbol,
                )['result']['list'][0]['avgPrice'])

                session.set_trading_stop(
                    category="linear",
                    symbol=symbol,
                    tpslMode = "Full",
                    takeProfit= f"{round(stop_price*0.970, 5)}",
                    stopLoss= f"{round(stop_price*1.011, 5)}",
                    positionIdx = 0
                )
                series["count"] = 0
                print(df)

            # Place buy order if MACD crosses below a threshold

            if histogram <= -trigger and key :
                order = session.place_order(
                    category="linear",
                    symbol=symbol,
                    side="Buy",
                    orderType="Market",
                    qty= f"{int(15/price)}",
                    price= f"{price}",
                    isLeverage=1,
                    orderFilter="tpslOrder",
                )
                print(order)
                stop_price = float(session.get_positions(
                    category="inverse",
                    symbol=symbol,
                )['result']['list'][0]['avgPrice'])

                session.set_trading_stop(
                    category="linear",
                    symbol=symbol,
                    tpslMode = "Full",
                    takeProfit= f"{round(stop_price*1.03, 5)}",
                    stopLoss= f"{round(stop_price*0.989, 5)}",
                    positionIdx = 0
                )
                count = 0
                print(df)
            
            sleep(0.1)
    
            count += 1
            
        except TimeoutError as e:
            # recoverable error, do nothing and retry later
            print(type(e).__name__, str(e))
        except ValueError as e:
            # recoverable error, you might want to sleep a bit here and retry later
            print(type(e).__name__, str(e))
        except exceptions.FailedRequestError as e:
            # recoverable error, you might want to sleep a bit here and retry later
            print(type(e).__name__, str(e))
        except OSError as e:
            # recoverable error, you might want to sleep a bit here and retry later
            print(type(e).__name__, str(e))
        except Exception as e:
            print(type(e).__name__, str(e))
            
