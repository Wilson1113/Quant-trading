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
    mean = df.mean()
    std = df.std()
    return 	2.9*std + mean

# Initialize the Bybit API client
session = HTTP(
    testnet = False,
    api_key = '0LVZwXtRF8uxbLpv5d', 
    api_secret ='sueHTB8wKAKXF60Fv7D0urf8aBUiMMN60HHX',
)


# Extract data for each candles
candles = session.get_kline(
    category="linear",
    symbol="HIFIUSDT",
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

df = pd.DataFrame(
    {
        "prices":prices,
        "volumns":volumns
    },
    index = times
)


# Calculating MACD
df["ema_26"] = df["prices"].ewm(span=26).mean()
df["ema_12"] = df["prices"].ewm(span=12).mean()
df["macd"] = df["ema_12"] - df["ema_26"]
df["signal"] = df["macd"].ewm(span=9).mean()
df["histogram"] = df["macd"] - df["signal"]

histogram_5 = df[df.index% 300000 == 240000]

if times[-1] % 300000 != 240000:
   # Add the last row to histogram_5
    new_hist =  pd.DataFrame([df.iloc[-1]])
    histogram_5 = pd.concat([histogram_5,new_hist])

# # Initialize variables
key = True

# Set initial count
count = 3001

# Set leverage
try:
    print(session.set_leverage(
        category="linear",
        symbol="HIFIUSDT",
        buyLeverage="15",
        sellLeverage="15",
    ))
except Exception as e:
    print(e)

while True:

    try:
        # Get real-time market price data
        orders = session.get_open_orders(
            category="linear",
            symbol="HIFIUSDT",
            openOnly=0,
            limit=1,
        )["result"]["list"]
        
        candle = session.get_kline(
            category="linear",
            symbol="HIFIUSDT",
            interval='1',
            limit=1,
        )["result"]["list"][0]

        price = float(candle[4])
        time = float(candle[0])

        key = False if len(orders) >= 1 or count < 3001 else True
 
    # Check for duplicate time and remove corresponding elements from lists
        if time == df.index[-1]:
            df_transpose = df.T
            df_transpose.pop(time)
            df = df_transpose.T
        

        if time% 300000 != 240000 or time == histogram_5.index[-1]:
            histogram_5_transpose = histogram_5.T
            histogram_5_transpose.pop(histogram_5.index[-1])
            histogram_5 = histogram_5_transpose.T

        # Update EMA values
        ema_12 = update_ema(price, 12,  df.iloc[-1]["ema_12"])
        ema_26 = update_ema(price, 26,  df.iloc[-1]["ema_26"])
        macd = ema_12 - ema_26
        signal = update_ema(macd, 9,  df.iloc[-1]["signal"])
        histogram = macd - signal
        new_row = {"ema_12": ema_12, "ema_26": ema_26, "macd": macd, "histogram": histogram, "signal": signal, "prices": price}
        # Create a new DataFrame for the new row
        new_row_df = pd.DataFrame([new_row],index=[time])
        
        # Concatenate the new DataFrame with the original DataFrame
        df = pd.concat([df, new_row_df])

        new_hist =  pd.DataFrame([df.iloc[-1]])
        histogram_5 = pd.concat([histogram_5,new_hist])
        
        # Calculate histogram values
        value = histogram_5.iloc[-1]["histogram"]

        # Calculate the trigger point
        trigger = get_95(histogram_5["histogram"])
        print(trigger)
        # Place sell order if MACD crosses above a threshold
        if value >= trigger and key:
            order = session.place_order(
                category="linear",
                symbol="HIFIUSDT",
                side="Sell",
                orderType="Market",
                qty= f"{int(15/value)}",
                price= f"{value}",
                isLeverage=1,
                orderFilter="tpslOrder",
            )
            print(order)
            stop_price = float(session.get_positions(
                category="inverse",
                symbol="HIFIUSDT",
            )['result']['list'][0]['avgPrice'])

            session.set_trading_stop(
                category="linear",
                symbol="HIFIUSDT",
                tpslMode = "Full",
                takeProfit= f"{round(stop_price*0.930, 5)}",
                stopLoss= f"{round(stop_price*1.025, 5)}",
                positionIdx = 0
            )
            count = 0

        # Place buy order if MACD crosses below a threshold

        if value <= -trigger and key :
            order = session.place_order(
                category="linear",
                symbol="HIFIUSDT",
                side="Buy",
                orderType="Market",
                qty= f"{int(15/value)}",
                price= f"{value}",
                isLeverage=1,
                orderFilter="tpslOrder",
            )
            print(order)
            stop_price = float(session.get_positions(
                category="inverse",
                symbol="HIFIUSDT",
            )['result']['list'][0]['avgPrice'])

            session.set_trading_stop(
                category="linear",
                symbol="HIFIUSDT",
                tpslMode = "Full",
                takeProfit= f"{round(stop_price*1.07, 5)}",
                stopLoss= f"{round(stop_price*0.975, 5)}",
                positionIdx = 0
            )
            count = 0
        
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

