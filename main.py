import time
import pandas as pd
import talib
from datetime import datetime
import csv
import traceback
import math
import logging
from datetime import date
import datetime
import requests
from fyers_api import fyersModel
from fyers_api import accessToken
from fyers_api.Websocket import ws
import os
import threading

symbol_list = ["NSE:NIFTY50-INDEX"]
symbol = "NSE:NIFTY50-INDEX"

# symbol_list = ["MCX:CRUDEOIL23MARFUT"]
# symbol = "MCX:CRUDEOIL23MARFUT"
#fyers config
# client_id = app_id = "4248K66WUR-100"
# secret_key = app_secret = "O3URMWM4J3"
client_id = app_id = "Z560I50PR6-100"
secret_key = app_secret = "AVRQW3Q7JW"
redirect_uri = redirect_url = "https://www.google.com"

# telegram token
TOKEN = "6154171898:AAG6Dt5_6CCcZwl8R9AG21mr32J10pY4Wgc"
chat_id = "930057549"
message ="this is test"

def send_telegram_message():
    file_location = f"{symbol}.csv"
    send_document = 'https://api.telegram.org/bot' + TOKEN + '/sendDocument?'
    data = {
        'chat_id': chat_id,
        'parse_mode': 'HTML',
        'caption': "todays update"
    }
    # Need to pass the document field in the files dict
    files = {
        'document': open(file_location, 'rb')
    }
    r = requests.post(send_document, data=data, files=files, stream=True)
    print(r.json())


def get_access_token():
    if not os.path.exists("accessToken2.txt"):
        session = accessToken.SessionModel(client_id=app_id, secret_key=app_secret, redirect_uri=redirect_url,
                                           response_type="code", grant_type="authorization_code")
        response = session.generate_authcode()
        print("Login URL = ", response)
        auth_code = input("Enter auth code ")
        session.set_token(auth_code)
        access_token = session.generate_token()["access_token"]
        print(response)
        with open("accessToken2.txt", "w") as f:
            f.write(access_token)
    else:
        print("Access token already exist")
        with open("accessToken2.txt", "r") as f:
            access_token = f.read()
    return access_token


access_Token = get_access_token()
fyers = fyersModel.FyersModel(client_id=client_id, token=access_Token,
                              log_path="/Users/aditya/PycharmProjects/pythonProject")


today = date.today()
header_list = ['Datetime', 'Symbol', 'Buy/Sell', 'Entry', 'Exit', 'Exit Date', "SL"]
with open(f'Logs/{symbol} {today}.csv', 'a', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n', fieldnames=header_list)
    writer.writeheader()
today = date.today()
logging.basicConfig(filename=f"Logs/{symbol} {today}.log",
                        filemode='a',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.NOTSET)

def start_web_socket():
    global fyersSocket
    global live_data
    live_data = {}
    ws_access_token = f"{app_id}:{access_Token}"
    data_type = "symbolData"

    def custom_message(msg):
        for symbol_data in msg:
            live_data["ltp"] = symbol_data['ltp']

    fyersSocket = ws.FyersSocket(access_token=ws_access_token,run_background=False,log_path="")
    fyersSocket.websocket_data = custom_message

    def subscribe_new_symbols(symbol_list):
        fyersSocket.subscribe(symbol=symbol_list,data_type=data_type)
    time.sleep(1)
    threading.Thread(target=subscribe_new_symbols,args=(symbol_list,)).start()


def candle_history():
    today_date = date.today()
    end_date = today_date - datetime.timedelta(days=70)
    data = {"symbol": symbol, "resolution": "5", "date_format": "1", "range_from": end_date,
            "range_to": today_date, "cont_flag": "1"}
    history_data = fyers.history(data)
    cols = ["datetime", "open", "high", "low", "close", "volume"]
    df = pd.DataFrame.from_dict(history_data["candles"])
    df.columns = cols
    df["datetime"] = pd.to_datetime(df["datetime"], unit="s")
    df["datetime"] = df["datetime"].dt.tz_localize('utc').dt.tz_convert('Asia/Kolkata')
    df["datetime"] = df["datetime"].dt.tz_localize(None)
    df['datetime'] = pd.to_datetime(df['datetime'], format="%Y-%m-%dT%H:%M:%S")
    return df


def five_ma():
    global df, global_candle_history
    df = candle_history()
    last_traded_price = live_data.get('ltp')
    df.iloc[-1, -2] = last_traded_price
    df["EMA_5"] = (talib.MA(df["close"], timeperiod=5))
    global_candle_history = df
    time.sleep(10)
    threading.Thread(target=five_ma,args=()).start()


def buy_sell_trade(buy_or_sell, last_traded_price, strike_price):
    logging.critical(f"buy/selll {buy_or_sell} ltp {last_traded_price} strike_name {strike_price}")

    if buy_or_sell != "sell":
        strike_price = int(math.ceil(last_traded_price / 100.0)) * 100

    difference = float(strike_price)-float(last_traded_price)
    if symbol == "NSE:FINNIFTY-INDEX":
        if difference > 50:
            strike_price = strike_price-50

        strike_name = "NSE:FINNIFTY23FEB{0}PE".format(strike_price)
        lot_size = 40
    elif symbol == "NSE:NIFTYBANK-INDEX":
        strike_name = "NSE:BANKNIFTY23302{0}PE".format(strike_price)
        lot_size = 25
    elif symbol == "NSE:NIFTY50-INDEX":
        if difference > 50 and buy_or_sell != "sell":
            strike_price = strike_price - 50

        strike_name = "NSE:NIFTY23MAR{0}PE".format(strike_price)
        lot_size = 50

    elif symbol == "MCX:CRUDEOIL23MARFUT":
        strike_name = symbol
        lot_size = 1

    else:
        print(f"Invalid Symbol {symbol}".format(symbol))

    if buy_or_sell == "buy":
        side = 1
        type = 1
    elif buy_or_sell == "sell":
        side = -1
        type = 2
    strike_price_ltp = get_ltp(strike_name)
    data = {
        "symbol": f"{strike_name}",
        "qty": f"{lot_size}",
        "type": 1,
        "side": f"{side}",
        "productType": "MARGIN",
        "limitPrice": f"{strike_price_ltp}",
        "stopPrice": 0,
        "validity": "DAY",
        "disclosedQty": 0,
        "offlineOrder": "False",
        "stopLoss": 0,
        "takeProfit": 0
    }
    response = fyers.place_order(data)
    print(response)
    return strike_price


def write_in_excel(trade):
    with open(f"{symbol}.csv", "a", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=header_list)
        writer.writerow(trade)


def get_ltp(symbol_to_check):
    data = {"symbol": symbol_to_check, "ohlcv_flag": "1"}
    depth = fyers.depth(data)
    try:
        api_message = pd.DataFrame.from_dict(depth).get('s')
        print(f"ERROR WHILE LTP {api_message}")
        logging.critical(f"ERROR WHILE LTP {api_message}")

    except Exception as ex:
        print(traceback.format_exc())
    df = pd.DataFrame.from_dict(depth["d"])
    df = df.T
    ltp = df.get("ltp")[0]
    return ltp


def trade(symbol):
    profit = loss = 0.0
    start_time = "09:21:00"
    end_time = "15:10:00"
    initial_one_is_to_three = None
    is_one_is_to_three_done = False
    trade = {"Datetime": None, "Symbol": None, "Buy/Sell": None, "Entry": None, "Exit": None, "Exit Date": None,
             "SL": None}
    position = None

    while datetime.datetime.now().strftime("%H:%M:%S") > start_time and datetime.datetime.now().strftime(
            "%H:%M:%S") < end_time:
        # while True:

        try:
            global strike_price
            last_traded_price = live_data.get('ltp')
            last_candle_high = float(global_candle_history.iloc[-1, -5])
            five_ema_value = float(global_candle_history.iloc[-1, -1])
            now = datetime.datetime.now()
            current_time = now.strftime("%H:%M:%S")
            second_last_candle_high = float(global_candle_history.iloc[-2, -5])
            second_last_candle_low = float(global_candle_history.iloc[-2, -4])
            second_last_ma_value = float(global_candle_history.iloc[-2, -1])
            last_trading_date_time = pd.Timestamp(current_time)
            # logging.info(" LTP : {0} SLCH : {1} SLCL : {2} SLMA : {3} LMA : {4}".format(
            #     last_traded_price, second_last_candle_high, second_last_candle_low, second_last_ma_value,
            #     five_ema_value))
            size_of_candle = second_last_candle_high - second_last_candle_low

            # sell scenario
            if position != "sell" and second_last_candle_high > second_last_ma_value and second_last_candle_low > second_last_ma_value and abs(
                    size_of_candle) < 31:
                print("%s :  ALERT C  LTP : %s, SL5MA : %s SLCH : %s SLCL %s" % (
                    last_trading_date_time, last_traded_price, second_last_ma_value, second_last_candle_high,
                    second_last_candle_low))
                if last_traded_price < second_last_candle_low:
                    if position != "sell":
                        trade["Symbol"] = symbol
                        trade["Buy/Sell"] = "sell"
                        trade["Entry"] = last_traded_price
                        trade["SL"] = second_last_candle_high
                        trade["Datetime"] = last_trading_date_time
                        df = pd.DataFrame([trade])
                        print(df)
                        write_in_excel(trade)
                        initial_one_is_to_three = 2 * (second_last_candle_high - last_traded_price)
                        one_is_to_three_ltp = last_traded_price - initial_one_is_to_three
                        print("1:3 target => {0}".format(one_is_to_three_ltp))
                        position = "sell"
                        strike_price = buy_sell_trade("buy", last_traded_price, strike_price=None)
                        logging.info(df)

            #   Exit trade
            if position == "sell":
                entry_price = float(trade["Entry"])
                stop_loss_price = float(trade["SL"])
                trailing_sl = second_last_candle_high
                if last_traded_price < one_is_to_three_ltp:
                    is_one_is_to_three_done = True
                if last_traded_price < one_is_to_three_ltp or is_one_is_to_three_done is True:
                    if last_traded_price >= stop_loss_price or last_traded_price >= trailing_sl:
                        print("2nd last candle high : {0} LTP : {1} 5MA ".format(
                            second_last_candle_high, last_candle_high, five_ema_value))
                        difference = float(last_traded_price - entry_price)
                        if difference > 0:
                            print("Exit position at LOSS : {0} at LTP : {1} ENTRY : %s TIME : {2}".format(
                                difference, last_trading_date_time, entry_price, last_trading_date_time))
                            trade["Symbol"] = symbol
                            trade["Buy/Sell"] = "Buy"
                            trade["Exit"] = last_traded_price
                            trade["Datetime"] = last_trading_date_time
                            is_one_is_to_three_done = False
                            buy_sell_trade("sell", last_traded_price, strike_price)
                            initial_one_is_to_three = None
                            strike_price = None
                            position = None
                            write_in_excel(trade)
                            logging.info(df)
                        if difference < 0:
                            print("Exit position at PROFIT : {0} at LTP : {1} ENTRY : {2} TIME : {3}".format(
                                difference, last_trading_date_time, entry_price, last_trading_date_time))
                            trade["Symbol"] = symbol
                            trade["Buy/Sell"] = "Buy"
                            trade["Exit"] = last_traded_price
                            trade["Datetime"] = last_trading_date_time
                            is_one_is_to_three_done = False
                            buy_sell_trade("sell", last_traded_price, strike_price)
                            position = None
                            initial_one_is_to_three = None
                            strike_price = None
                            write_in_excel(trade)
                            logging.info(df)
                    elif trade["SL"] < last_traded_price:
                        difference = float(trade["SL"]) - last_traded_price
                        trade["Symbol"] = symbol
                        trade["Buy/Sell"] = "Buy"
                        trade["Exit"] = last_traded_price
                        trade["Datetime"] = last_traded_price
                        print("1:3 Exit position at LOSS : {0} at LTP : {1} EXIT : {2} TIME : {3}".format(
                            difference, last_traded_price, float(trade["SL"]), current_time))
                        write_in_excel(trade)
                        buy_sell_trade("sell", last_traded_price, strike_price)
                        is_one_is_to_three_done = False
                        position = None
                        initial_one_is_to_three = None
                        trade = {"Datetime": None, "Symbol": None, "Buy/Sell": None, "Entry": None, "Exit": None,
                                 "Exit Date": None, "SL": None}
                        strike_price = None

                        logging.info(df)
                elif last_traded_price > trade["SL"] and is_one_is_to_three_done is False:
                    difference = float(trade["SL"]) - entry_price
                    trade["Symbol"] = symbol
                    trade["Buy/Sell"] = "Buy"
                    trade["Exit"] = last_traded_price
                    trade["Datetime"] = current_time
                    buy_sell_trade("sell", last_traded_price, strike_price)
                    print("1:3 Exit position at LOSS : {0} at LTP : {1} EXIT : {2} TIME : {3}".format(
                        difference, last_traded_price, float(trade["SL"]), current_time))
                    initial_oneIsToThree = None
                    trade = {"Datetime": None, "Symbol": None, "Buy/Sell": None, "Entry": None, "Exit": None,
                             "Exit Date": None, "SL": None}
                    strike_price = None
                    loss = loss + difference
                    position = None
                    # print("")
            time.sleep(1.4)

        except Exception as ex:
            print(traceback.format_exc())

    send_telegram_message()


if __name__ == "__main__":
    # buy_sell_trade("buy", 17548)
    start_web_socket()
    five_ma()
    trade(symbol=symbol)
