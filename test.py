import os
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timedelta
from datetime import datetime as date
import warnings
import requests
from io import BytesIO
import base64
import jwt
import plotly.express as px
import quantstats as qs

from lumibot.strategies import Strategy
from lumibot.backtesting import YahooDataBacktesting

from lumibot.traders import Trader

import sqlalchemy as sql
from sqlalchemy import text
from sqlalchemy_utils import database_exists, create_database

# deactivate warnings
warnings.filterwarnings('ignore')

IS_BACKTESTING = True
REPORTS = False
REPORTS_FOLDER      = os.getcwd() + '/reports/'
ASSET_FOLDER        = os.getcwd() + '/db/'
LOGS_Folder         = os.getcwd() + '/logs/'
DB_NAME             = ASSET_FOLDER + 'lb-etf.db'
FIRST_TRADE = 0.08
NEXT_TRADE = 0.02
QTY1FROMopen = 25

admin_api_key = "65fb19c037ea7e00014df6aa:bd4755a083655540e802eded3e0330526ddf47b6f7e9938d4194de600a969f2b"
api_url = "https://rene-richartz.ghost.io"
post_route = "/ghost/api/admin/posts/?source=html&formats=html,lexical&include=tags"
media_route = "/ghost/api/admin/images/upload/" 


# create folders if not exist
if not os.path.exists(REPORTS_FOLDER):
    os.makedirs(REPORTS_FOLDER)

if not os.path.exists(ASSET_FOLDER):
    os.makedirs(ASSET_FOLDER)

if not os.path.exists(LOGS_Folder):
    os.makedirs(LOGS_Folder)

db_run = "sqlite:///{}".format(DB_NAME)
print(db_run)
if not database_exists(db_run):
    create_database(db_run)
engine = sql.create_engine(db_run) #engine = sql.create_engine('sqlite://', echo=False)

# delete all files in log folder
files = os.listdir(LOGS_Folder)
for file in files:
    os.remove(LOGS_Folder + file)

class etf(Strategy):
    def initialize(self):
        if self.is_backtesting:
            self.sleeptime = "1D"

            self.db_create_strategies_table()

            self.db_create_signals_table()
            self.db_drop_signals_table()

            self.db_create_trades_open_table()
            self.db_drop_trade_open_table()

            self.db_create_trades_closed_table()
            self.db_drop_trade_closed_table()

            self.db_create_stats_table()
            self.db_drop_stats_table()

            self.db_create_strategy_stats_table()
            self.db_drop_strategy_stats_table()


        self.historical_bars = 100
        self.TICKERS = ['TQQQ','UDOW','MEXX', 'UMDD', 'DIG','TMF','UPRO','URTY','USD','YCS']

    def on_trading_iteration(self):
        today = self.get_datetime()
        weekday = today.weekday()
        week = today.isocalendar()[1]
        year = today.isocalendar()[0]
        today_string = today.strftime("%Y-%m-%d")
    
        # tqqq
        self.log_message("Running TQQQ Strategy")
        self.test(today_string)


        # Here we start selling the stocks
        signals_exit = self.db_exit_signals(today_string)
        for strategy in signals_exit.index:
            symbol = signals_exit.loc[strategy, 'asset']
            open_price = signals_exit.loc[strategy, 'open_price']
            quantity = signals_exit.loc[strategy, 'quantity']
            win = signals_exit.loc[strategy, 'win']
            loss = signals_exit.loc[strategy, 'loss']
            profit = signals_exit.loc[strategy, 'profit']
            
            price = self.get_last_price(symbol)
                
            if price > open_price:
                win += 1
            else:
                loss += 1
                
            profit = round(profit + ((price - open_price) * quantity),2)
            
            order = self.create_order(symbol, quantity, "sell")
            self.submit_order(order)
            if not self.is_backtesting:
                self.wait_for_order_execution(order)
            self.db_close_trade(strategy, win, loss, profit)
            self.db_insert_trade_closed(today, symbol, strategy, "sell", quantity, price)

        # Here we start buying the stocks
        signals_entry = self.db_entry_signals(today_string)
        for strategy in signals_entry.index:
            symbol = signals_entry.loc[strategy, 'asset']
            winrate = signals_entry.loc[strategy, 'winrate']
            win = signals_entry.loc[strategy, 'win']
            loss = signals_entry.loc[strategy, 'loss']
            self.log_message(f"Strategy: {strategy} {symbol} {winrate} {win} {loss}")
            price = self.get_last_price(symbol)
            
            open_trades = self.db_get_num_open_positions()
            if open_trades == 0:
                percent_invest = FIRST_TRADE
            else:
                percent_invest = FIRST_TRADE + (NEXT_TRADE * open_trades)

            percent_invest = 1 / 7

            if price > 0 and percent_invest > 0:    
                budget = self.get_cash() * percent_invest
                quantity = int(budget / price)
                '''if open_trades >= QTY1FROMopen:
                    quantity = 1'''

            #quantity = 1

            if quantity > 0:
                '''if winrate < 1.8:
                    quantity = 1'''

                order = self.create_order(symbol, quantity, "buy")
                self.submit_order(order)
                if not self.is_backtesting:
                    self.wait_for_order_execution(order)
                self.db_open_trade(strategy, price, quantity, today_string)
                self.db_insert_trade_open(today, symbol, strategy, "buy", quantity, price, winrate, open_trades, percent_invest)
            else:
                self.log_message(f"ALERT : Quantity is 0 {symbol} {price} {percent_invest}")

        # Daily Trades report
        if REPORTS:
            if not self.is_backtesting:
                self.log_message("Posting trades of the day")
                myReport = self.db_report_trades_of_day(today_string)

            if self.is_backtesting:   
                if today_string == '2024-03-27':
                    self.log_message("Posting trades of the day")
                    myReport = self.db_report_trades_of_day(today_string)

        myCash = self.get_cash()
        Value = self.get_portfolio_value()
        StockValue = Value - myCash

        self.db_insert_stats(today, myCash, StockValue, Value)

        self.db_command("Update strategies set aging = aging + 1 where open = 1")
        self.db_command("Update strategies set avg_days_trade = aging / (win + loss)")

        # Update Value in strategies
        for ticker in self.TICKERS:
            price = self.get_last_price(ticker)
            if price is not None:
                self.db_command(f"Update strategies set value = quantity * {price} where symbol = '{ticker}'")

        # create strategy statistics
        stratvalues = self.db_get_strategies_values()
        for strategy in stratvalues.index:
            symbol = stratvalues.loc[strategy, 'symbol']
            winrate = stratvalues.loc[strategy, 'winrate']
            open = stratvalues.loc[strategy, 'open']
            profit = stratvalues.loc[strategy, 'profit']
            avg_days_trade = stratvalues.loc[strategy, 'avg_days_trade']
            self.db_insert_strategy_stats(today_string, symbol, strategy, winrate, profit, avg_days_trade, open)
            #date, symbol, strategy, winrate, profit, avg_days_trade, open


        # weekly report
        if REPORTS:
            if not self.is_backtesting:        
                #weekday = today.weekday()
                if weekday == 5:
                    self.db_report_week(year, week, myCash)

            if self.is_backtesting:   
                if today_string == '2024-03-27':
                    week = week - 1
                    self.db_report_week(year, week, myCash)

        # Sleep until market opens
        if not self.is_backtesting:
            time2open = self.broker.get_time_to_open()
            self.log_message(f"Sleeping until market opens {time2open}")
            self.sleep(time2open)

    def test(self, date):
        mySymbol = "UMDD"
        bars =  self.get_historical_prices(mySymbol, self.historical_bars, "day")
        df = bars.df
        self.log_message(f"Running test for {mySymbol} {date}")

        if len(df) < self.historical_bars:
            return

        Portfolio = "13.106.126.142.146"
        myStrategy = "2.68.134"
        # Entry Condition: (Aroon(Main chart, 67, 0)[1] crosses below Aroon(Main chart, 16, 0)[1])
        aroon1 = ta.aroon(df['high'], df['low'], 67)
        aroon2 = ta.aroon(df['high'], df['low'], 16)
        entry = self.cross_below(aroon1['AROOND_67'], aroon2['AROOND_16'])

        # Exit Condition: (Aroon(Main chart, 18, 0)[1] crosses below Aroon(Main chart, 4, 1)[1])
        aroon1 = ta.aroon(df['high'], df['low'], 18)
        aroon2 = ta.aroon(df['high'], df['low'], 4)
        exit = self.cross_below(aroon1['AROOND_18'], aroon2['AROONU_4'])

        if entry or exit:
            self.db_insert_signal(mySymbol, myStrategy, entry, exit, date)

        myStrategy = "3.30.115"
        # Entry Condition: (Linear Regression(Main chart, PRICE_CLOSE, 18)[1] crosses below Triple Exponential Moving Average(Main chart, PRICE_CLOSE, 14)[1])
        linreg = ta.linreg(df['close'], 18)
        tema = ta.tema(df['close'], 14)
        entry = self.cross_below(linreg, tema)

        # Exit Condition: (Double Exponential Moving Average(Main chart, PRICE_CLOSE, 74)[1] crosses below Linear Regression(Main chart, PRICE_CLOSE, 4)[1])
        dema = ta.dema(df['close'], 74)
        linreg = ta.linreg(df['close'], 4)
        exit = self.cross_below(dema, linreg)

        if entry or exit:
            self.db_insert_signal(mySymbol, myStrategy, entry, exit, date)

        myStrategy = "4.26.169"
        # Entry Condition: (Money Flow Index(Main chart, 68)[1] crosses below Money Flow Index(Main chart, 76)[1])
        mfi1 = ta.mfi(df['high'], df['low'], df['close'], df['volume'], 68)
        mfi2 = ta.mfi(df['high'], df['low'], df['close'], df['volume'], 76)
        entry = self.cross_below(mfi1, mfi2)

        # Exit Condition: (Chande Momentum Oscillator(Main chart, PRICE_CLOSE, 24)[1] crosses above Chande Momentum Oscillator(Main chart, PRICE_CLOSE, 68)[1])
        cmo1 = ta.cmo(df['close'], 24)
        cmo2 = ta.cmo(df['close'], 68)
        exit = self.cross_above(cmo1, cmo2)

        if entry or exit:
            self.db_insert_signal(mySymbol, myStrategy, entry, exit, date)

        myStrategy = "3.32.138"
        # Entry Condition: (Chande Momentum Oscillator(Main chart, PRICE_CLOSE, 25)[1] crosses below Chande Momentum Oscillator(Main chart, PRICE_CLOSE, 10)[1])
        cmo1 = ta.cmo(df['close'], 25)
        cmo2 = ta.cmo(df['close'], 10)
        entry = self.cross_below(cmo1, cmo2)

        # Exit Condition: (Triple Exponential Moving Average(Main chart, PRICE_CLOSE, 9)[1] crosses above Double Exponential Moving Average(Main chart, PRICE_CLOSE, 57)[1])
        tema = ta.tema(df['close'], 9)
        dema = ta.dema(df['close'], 57)
        exit = self.cross_above(tema, dema)
        
        if entry or exit:
            self.db_insert_signal(mySymbol, myStrategy, entry, exit, date)

        myStrategy = "4.38.120"
        # Entry Condition: (Williams' %R(Main chart, 18)[1] crosses below Williams' %R(Main chart, 4)[1])
        willr1 = ta.willr(df['high'], df['low'], df['close'], 18)
        willr2 = ta.willr(df['high'], df['low'], df['close'], 4)
        entry = self.cross_below(willr1, willr2)

        # Exit Condition: (Double Exponential Moving Average(Main chart, PRICE_CLOSE, 50)[1] crosses below Triple Exponential Moving Average (T3)(Main chart, PRICE_CLOSE, 3, 0.66)[1])
        dema = ta.dema(df['close'], 50)
        t3 = ta.t3(df['close'], 3, 0.66)
        exit = self.cross_below(dema, t3)

        if entry or exit:
            self.db_insert_signal(mySymbol, myStrategy, entry, exit, date)




    def cross_below(self, df_a, df_b):
        return df_a.iloc[-2] > df_b.iloc[-2] and df_a.iloc[-1] < df_b.iloc[-1]
    
    def cross_above(self, df_a, df_b):
        return df_a.iloc[-2] < df_b.iloc[-2] and df_a.iloc[-1] > df_b.iloc[-1]

    def db_command(self, command):
        self.log_message(f"Running command: {command}")
        # update strategies
        with engine.connect() as con:
            sqltext = text(command)
            con.execute(sqltext)
            con.commit()

    def db_create_strategies_table(self):
        self.log_message("Creating strategies table")
        strategies= [
            {"portfolio": "13.106.126.142.146", "strategy": "2.68.134", "symbol": "UMDD", "entry": False, "exit": False, "open": False, "win": 0, "loss": 0, "winrate": 0.0, "value": 0.0, "quantity": 0.0, "profit": 0.0,  "aging": 0, "value": 0.0, "EntryDate": "", "avg_days_trade": 0.0, "open_price": 0.0, },
            {"portfolio": "13.106.126.142.146", "strategy": "3.30.115", "symbol": "UMDD", "entry": False, "exit": False, "open": False, "win": 0, "loss": 0, "winrate": 0.0, "value": 0.0, "quantity": 0.0, "profit": 0.0,  "aging": 0, "value": 0.0, "EntryDate": "", "avg_days_trade": 0.0, "open_price": 0.0, },
            {"portfolio": "13.106.126.142.146", "strategy": "4.26.169", "symbol": "UMDD", "entry": False, "exit": False, "open": False, "win": 0, "loss": 0, "winrate": 0.0, "value": 0.0, "quantity": 0.0, "profit": 0.0,  "aging": 0, "value": 0.0, "EntryDate": "", "avg_days_trade": 0.0, "open_price": 0.0, },
            {"portfolio": "13.106.126.142.146", "strategy": "3.32.138", "symbol": "UMDD", "entry": False, "exit": False, "open": False, "win": 0, "loss": 0, "winrate": 0.0, "value": 0.0, "quantity": 0.0, "profit": 0.0,  "aging": 0, "value": 0.0, "EntryDate": "", "avg_days_trade": 0.0, "open_price": 0.0, },
            {"portfolio": "13.106.126.142.146", "strategy": "4.38.120", "symbol": "UMDD", "entry": False, "exit": False, "open": False, "win": 0, "loss": 0, "winrate": 0.0, "value": 0.0, "quantity": 0.0, "profit": 0.0,  "aging": 0, "value": 0.0, "EntryDate": "", "avg_days_trade": 0.0, "open_price": 0.0, },

        ]

        strategies = pd.DataFrame(strategies)
        strategies.set_index('strategy', inplace=True)
        strategies.to_sql('strategies', con=engine, if_exists='replace', index_label='strategy')

    def db_create_signals_table(self):
        with engine.connect() as con:
            sqltext = text('CREATE TABLE IF NOT EXISTS signals (id INTEGER PRIMARY KEY, asset TEXT, strategy TEXT, entry BOOLEAN, exit BOOLEAN, date TEXT)')
            con.execute(sqltext)
            con.commit()

    def db_drop_signals_table(self):
        with engine.connect() as con:
            sqltext = text("delete from signals")
            con.execute(sqltext)
            con.commit()

    def db_insert_signal(self, asset, strategy, entry, exit, date):
        with engine.connect() as con:
            sqltext = text(f"INSERT INTO signals (asset, strategy, entry, exit, date) VALUES ('{asset}', '{strategy}', {entry}, {exit}, '{date}')")
            con.execute(sqltext)
            con.commit()

    def db_get_status_signals(self, date):
        myCommand = (f"select signals.asset, \
                signals.strategy, \
                signals.entry, \
                signals.exit, \
                strategies.win, \
                strategies.loss, \
                strategies.winrate, \
                strategies.open_price, \
                strategies.quantity, \
                strategies.profit \
                from signals \
                inner join strategies on signals.strategy = strategies.strategy \
                where signals.date = '{date}' \
                ORDER BY strategies.winrate DESC")
        sql = text(myCommand)
        data = pd.read_sql_query(sql , engine)
        return data

    def db_exit_signals(self, date):
        myCommand = (f"select distinct \
                signals.strategy, \
                signals.asset, \
                signals.entry, \
                signals.exit, \
                strategies.win, \
                strategies.loss, \
                strategies.winrate, \
                strategies.open_price, \
                strategies.quantity, \
                strategies.profit \
                from signals \
                inner join strategies on signals.strategy = strategies.strategy \
                where signals.date = '{date}' and signals.exit = 1 and strategies.open = 1")
        sql = text(myCommand)
        data = pd.read_sql_query(sql , engine, index_col='strategy')
        return data

    def db_entry_signals(self, date):
        myCommand = (f"select signals.asset, \
                signals.strategy, \
                signals.entry, \
                signals.exit, \
                strategies.win, \
                strategies.loss, \
                strategies.winrate, \
                strategies.open_price, \
                strategies.quantity, \
                strategies.profit \
                from signals \
                inner join strategies on signals.strategy = strategies.strategy \
                where signals.date = '{date}' and signals.entry = 1 and strategies.open = 0 \
                ORDER BY strategies.winrate DESC")
        sql = text(myCommand)
        data = pd.read_sql_query(sql , engine, index_col='strategy')
        return data

    def db_create_trades_open_table(self):
        with engine.connect() as con:
            sqltext = text('CREATE TABLE IF NOT EXISTS Trades_open (id INTEGER PRIMARY KEY, date datetime, year integer, week integer, symbol text, strategy text, direction text, quantity float, price float, cur_winrate float, cur_positions integer, cur_investment float)')
            con.execute(sqltext)
            con.commit()

    def db_drop_trade_open_table(self):
        with engine.connect() as con:
            sqltext = text("delete from Trades_open")
            con.execute(sqltext)
            con.commit()

    def db_insert_trade_open(self, date, symbol, strategy, direction, quantity, price, cur_winrate, cur_positions, cur_investment):
        week = date.isocalendar()[1]
        year = date.isocalendar()[0]
        myDate = date.strftime('%Y-%m-%d')

        with engine.connect() as con:
            sqltext = text(f"INSERT INTO Trades_open (date, year, week, symbol, strategy, direction, quantity, price, cur_winrate, cur_positions, cur_investment) VALUES ('{myDate}','{year}','{week}', '{symbol}', '{strategy}', '{direction}', '{quantity}', '{round(price,2)}', '{cur_winrate}', '{cur_positions}', '{cur_investment}')")
            con.execute(sqltext)
            con.commit()

    def db_create_trades_closed_table(self):
        with engine.connect() as con:
            sqltext = text('CREATE TABLE IF NOT EXISTS Trades_close (id INTEGER PRIMARY KEY, date datetime, year integer, week integer, symbol text, strategy text, direction text, quantity float, price float, open_id integer)')
            con.execute(sqltext)
            con.commit()

    def db_drop_trade_closed_table(self):
        with engine.connect() as con:
            sqltext = text("delete from Trades_close")
            con.execute(sqltext)
            con.commit()

    def db_insert_trade_closed(self, date, symbol, strategy, direction, quantity, price):
        week = date.isocalendar()[1]
        year = date.isocalendar()[0]
        myDate = date.strftime('%Y-%m-%d')

        with engine.connect() as con:
            sqltext = text(f"INSERT INTO Trades_close (date, year, week, symbol, strategy, direction, quantity, price,  open_id) VALUES ('{myDate}','{year}','{week}', '{symbol}', '{strategy}', '{direction}', '{quantity}', '{round(price,2)}', 0)")
            con.execute(sqltext)
            con.commit()

        with engine.connect() as con:
            sqltext = text(f"update Trades_close SET open_id = (select id from trades_open where symbol = '{symbol}' and strategy = '{strategy}' order by id desc limit 1) where symbol = '{symbol}' and strategy = '{strategy}' and open_id = 0") 
            con.execute(sqltext)
            con.commit()

    def db_open_trade(self, strategy, price, quantity, EntryDate):
        value = round(price * quantity,2)
        myCommand = (f"UPDATE strategies \
                        SET open = true , \
                        entry = false, \
                        open_price = {price}, \
                        value = {value}, \
                        EntryDate = '{EntryDate}', \
                        quantity = {quantity}  \
                        where strategy = '{strategy}'")
        sqltext = text(myCommand)
        with engine.connect() as con:
            con.execute(sqltext)
            con.commit()

    def db_close_trade(self, strategy, win, loss, profit):
        if win > 0 and loss > 0:
            winrate = round((win / loss), 1)
        else:
            winrate = 0.0

        myCommand = (f"UPDATE strategies SET \
                            open = false, \
                            exit = false, \
                            open_price = 0.0, \
                            quantity = 0, \
                            EntryDate = '', \
                            win = {win}, \
                            loss = {loss}, \
                            winrate = {winrate}, \
                            profit = {profit} \
                            WHERE strategy = '{strategy}'")
        sqltext = text(myCommand)
        with engine.connect() as con:
            con.execute(sqltext)
            con.commit()

    def db_get_num_open_positions(self):
        myCommand = (f"select count(strategy) as Anzahl from strategies where open = 1")
        sql = text(myCommand)
        data = pd.read_sql_query(sql , engine)
        myValue = 0.0
        for tmp in data.index:
            myValue = data.loc[tmp, 'Anzahl']
            
        return myValue 

    def db_create_stats_table(self):
        with engine.connect() as con:
            sqltext = text('CREATE TABLE IF NOT EXISTS AccountStats (id INTEGER PRIMARY KEY, date datetime, Year Integer, Week Integer, Cash float, Portfolio float, Total float)')
            con.execute(sqltext)
            con.commit()

    def db_drop_stats_table(self):
        with engine.connect() as con:
            sqltext = text("delete from AccountStats")
            con.execute(sqltext)
            con.commit()

    def db_insert_stats(self, date, cash, portfolio, total):
        week = date.isocalendar()[1]
        year = date.isocalendar()[0]
        myDate = date.strftime('%Y-%m-%d')

        with engine.connect() as con:
            sqltext = text(f"INSERT INTO AccountStats (date, Year, Week, Cash, Portfolio, Total) VALUES ('{myDate}', '{year}', '{week}', '{cash}', '{portfolio}', '{total}')")
            con.execute(sqltext)
            con.commit()

    def db_get_strategies_values(self):
        myCommand = (f"select strategy, symbol, open, winrate, profit, avg_days_trade from main.strategies")
        sql = text(myCommand)
        data = pd.read_sql_query(sql , engine,  index_col='strategy')
        return data

    def db_insert_strategy_stats(self, date, symbol, strategy, winrate, profit, avg_days_trade, open):
        with engine.connect() as con:
            sqltext = text(f"INSERT INTO strategy_stats (date, symbol, strategy, winrate, profit, avg_days_trade, open) VALUES ('{date}', '{symbol}', '{strategy}', '{winrate}', '{profit}', '{avg_days_trade}', {open})")
            con.execute(sqltext)
            con.commit()

    def db_create_strategy_stats_table(self):
        with engine.connect() as con:
            sqltext = text('CREATE TABLE IF NOT EXISTS strategy_stats (id INTEGER PRIMARY KEY, date datetime, symbol text, strategy TEXT, winrate FLOAT, profit FLOAT, avg_days_trade FLOAT, open BOOLEAN)')
            con.execute(sqltext)
            con.commit()

    def db_drop_strategy_stats_table(self):
        with engine.connect() as con:
            sqltext = text("delete from strategy_stats")
            con.execute(sqltext)
            con.commit()

    def db_insert_strategy_stats(self, date, symbol, strategy, winrate, profit, avg_days_trade, open):
        with engine.connect() as con:
            sqltext = text(f"INSERT INTO strategy_stats (date, symbol, strategy, winrate, profit, avg_days_trade, open) VALUES ('{date}', '{symbol}', '{strategy}', '{winrate}', '{profit}', '{avg_days_trade}', {open})")
            con.execute(sqltext)
            con.commit()

    def get_headers(self):
        api_id, api_secret = admin_api_key.split(':')
        iat = int(date.now().timestamp())
        headers = {"alg": "HS256", "typ": "JWT", "kid": api_id}
        payload = {"iat": iat, "exp": iat + 5 * 60, "aud": "/admin/"}    

        token = jwt.encode(payload, bytes.fromhex(api_secret), algorithm="HS256", headers=headers)

        auth_headers = {"Authorization": f"Ghost {token}"}

        return auth_headers

    def ghost_file(self, file_path, filename):
        #print("Uploading file to Ghost")
        #print(file_path)
        headers = self.get_headers()

        with open(file_path, "rb") as file:
            files = {'file': (f"{filename}", file, 'image/png')}
            r = requests.post(api_url + media_route, files=files, headers=headers)
        #print(r.json())
        url = r.json()['images'][0]['url']
        return url

    def ghost_post(self, message, header, short_message, post_type="backtest"):
        headers = self.get_headers()

        #data_uri = base64.b64encode(open(image_url, 'rb').read()).decode('utf-8')
        #img_tag = '<img src="data:image/png;base64,{0}">'.format(data_uri)

        if post_type == "backtest":
            data = {
                "posts": [
                    {
                        "title": f"{header}",
                        "html": f"{message}",
                        "custom_excerpt": f"{short_message}",
                        "status": "published",
                        "tags":[
                            {
                            "slug":"etf-trend-trader",
                            },
                            {
                            "slug":"backtest-result",
                            }
                        ],
                        "primary_tag": {
                            "slug":"backtest-result",
                        }
                    }
                ]
            } 

        if post_type == "weekly":
            data = {
                "posts": [
                    {
                        "title": f"{header}",
                        "html": f"{message}",
                        "custom_excerpt": f"{short_message}",
                        "status": "published",
                        "tags":[
                            {
                            "slug":"etf-trend-trader",
                            },
                            {
                            "slug":"weekly-report",
                            }
                        ],
                        "primary_tag": {
                            "slug":"weekly-report",
                        }
                    }
                ]
            } 

        if post_type == "trades":
            data = {
                "posts": [
                    {
                        "title": f"{header}",
                        "html": f"{message}",
                        "custom_excerpt": f"{short_message}",
                        "status": "published",
                        "visibility":"paid",
                        "tags":[
                            {
                            "slug":"etf-trend-trader",
                            },
                            {
                            "slug":"daily-trades",
                            }
                        ],
                        "primary_tag": {
                            "slug":"daily-trades",
                        }
                    }
                ]
            } 

        #print(data)

        response = requests.post(
            api_url + post_route,
            headers=headers,
            json=data
        )

        #response2 = requests.get(api_url + post_route, headers=headers)
        #print(response2.json())
        return response.json()

    def create_stats(self):
        stats = pd.DataFrame()
        stats = pd.read_sql_table('AccountStats', con=engine, index_col='date')
        len_stats = len(stats)

        portfolio_returns = stats['Total'].pct_change()  # Assuming 'value' column exists
        #spy_returns = qs.utils.download_returns('SPY')

        # rename stats colums
        stats.rename(columns={'Total': 'ETF'}, inplace=True)

        
        # Now you can compare your portfolio to SPY
        # Generate a report comparing them

        out_file = f"./reports/comparison_report.html"
        qs.reports.html(portfolio_returns, output=out_file, title='ETF Trend')
        #message.wp_post("ETF Trend vs. SPY", out_file, "publish", categories=[3,5])
        
        #qs.plots.snapshot(portfolio_returns, title='ETF Trend vs. SPY', show=False)
        out_file = f"./reports/yearly_returns.png"
        qs.plots.yearly_returns(portfolio_returns, savefig=out_file, show=False)
        #yearly_id, yearly_url =  message.wp_file(out_file)
        yearly_url = self.ghost_file(out_file, f"yearly_returns.png")

        out_file = f"./reports/drawdown.png"
        qs.plots.drawdown(portfolio_returns, savefig=out_file, show=False)
        #drawdown_id, drawdown_url =  message.wp_file(out_file)
        drawdown_url = self.ghost_file(out_file, f"drawdown.png")
        
        out_file = f"./reports/monthly_returns.png"
        qs.plots.monthly_returns(portfolio_returns, savefig=out_file, show=False)
        #monthly_id, monthly_url =  message.wp_file(out_file)
        monthly_url = self.ghost_file(out_file, f"monthly_returns.png")

        out_file = f"./reports/returns.png"
        qs.plots.returns(portfolio_returns,  savefig=out_file, show=False)
        #returns_id, returns_url =  message.wp_file(out_file)
        returns_url = self.ghost_file(out_file, f"returns.png")

        first_date = stats.index[0].strftime('%Y-%m-%d')
        last_date = stats.index[-1].strftime('%Y-%m-%d')

        myHTMLTable = ""
        myHTMLTable += "    <table class='tg'>"
        myHTMLTable += '    <colgroup>'
        myHTMLTable += "    <col style='text-align:left;'/>"
        #myHTMLTable += "    <col style='text-align:right;'/>"
        myHTMLTable += "    <col style='text-align:right;'/>"
        myHTMLTable += '    </colgroup>'
        myHTMLTable += ''
        myHTMLTable += '    <thead>'
        myHTMLTable += '    <tr>'
        myHTMLTable += f"        <th style='text-align:left;'>{first_date } to {last_date}</th>"
        #myHTMLTable += "        <th style='text-align:right;' width=150>SPY</th>"
        myHTMLTable += "        <th style='text-align:right;' width=150>ETF Trend Trader</th>"
        myHTMLTable += '    </tr>'
        myHTMLTable += '    </thead>'
        myHTMLTable += ''
        myHTMLTable += '    <tbody>'


        #cagr
        cagr_portfolio = round(qs.stats.cagr(portfolio_returns) * 100,2)
        myHTMLTable += '    <tr>'
        myHTMLTable += "        <td  style='text-align:left;'>CAGR</td>"
        myHTMLTable += f"        <td  style='text-align:right;'>{cagr_portfolio} %</td>"
        myHTMLTable += '    </tr>'

        #profit factor
        pf_portfolio = round(qs.stats.profit_factor(portfolio_returns),2)
        myHTMLTable += '    <tr>'
        myHTMLTable += "        <td style='text-align:left;'>Profit Factor</td>"
        myHTMLTable += f"        <td style='text-align:right;'>{pf_portfolio}</td>"
        myHTMLTable += '    </tr>'

        #profit ratio
        pr_portfolio = round(qs.stats.profit_ratio(portfolio_returns),2)
        myHTMLTable += '    <tr>'
        myHTMLTable += "        <td  style='text-align:left;'>Profit Ratio</td>"
        myHTMLTable += f"        <td  style='text-align:right;'>{pr_portfolio}</td>"
        myHTMLTable += '    </tr>'

        #payoff ratio
        por_portfolio = round(qs.stats.payoff_ratio(portfolio_returns),2)
        myHTMLTable += '    <tr>'
        myHTMLTable += "        <td style='text-align:left;'>Payoff Ratio</td>"
        myHTMLTable += f"        <td style='text-align:right;'>{por_portfolio}</td>"
        myHTMLTable += '    </tr>'

        # Sharp Ratio
        sharp_portfolio = round(qs.stats.sharpe(portfolio_returns),2)
        myHTMLTable += '    <tr>'
        myHTMLTable += "        <td  style='text-align:left;'>Sharpe Ratio</td>"
        myHTMLTable += f"       <td  style='text-align:right;'>{sharp_portfolio}</td>"
        myHTMLTable += '    </tr>'

        # max drawdown
        md_portfolio = round(qs.stats.max_drawdown(portfolio_returns) * 100,2)
        myHTMLTable += '    <tr>'
        myHTMLTable += "        <td style='text-align:left;'>Max Drawdown</td>"
        myHTMLTable += f"        <td style='text-align:right;'>{md_portfolio} %</td>"
        myHTMLTable += '    </tr>'

        # anualized volatility
        av_portfolio = round(qs.stats.volatility(portfolio_returns) * 100,2)
        myHTMLTable += '    <tr>'
        myHTMLTable += "        <td  style='text-align:left;'>Anualized Volatility</td>"
        myHTMLTable += f"        <td  style='text-align:right;'>{av_portfolio} %</td>"
        myHTMLTable += '    </tr>'

        myHTMLTable += '    </tbody>'
        myHTMLTable += '    </table>'
        
        #return myMarkDownTable, returns_id, returns_url, monthly_url, yearly_url, drawdown_url, first_date, last_date
        return myHTMLTable, returns_url, monthly_url, yearly_url, drawdown_url, first_date, last_date

    def db_strategy_values(self):
        myCommand = (f"select symbol, sum(value) as value from main.strategies group by symbol")
        sql = text(myCommand)
        data = pd.read_sql_query(sql , engine) # , index_col='symbol'
        return data

    def db_get_winloss(self):
        myCommand = (f"select symbol, sum(win) as win, sum(loss) as loss from main.strategies group by symbol")
        sql = text(myCommand)
        data = pd.read_sql_query(sql , engine)
        return data

    def db_get_winloss_strategy(self):
        myCommand = (f"select symbol, strategy,  win, loss from main.strategies")
        sql = text(myCommand)
        data = pd.read_sql_query(sql , engine)
        return data
    
    def get_symbol_statistics(self):
        myCommand = (f"select symbol, sum(win) as win, sum(loss) as loss, sum(win + loss) as total from main.strategies group by symbol;")
        sql = text(myCommand)
        data = pd.read_sql_query(sql , engine, index_col='symbol')

        myHTMLTable = ""
        myHTMLTable += "    <table >"
        myHTMLTable += '    <colgroup>'
        myHTMLTable += "    <col style='text-align:left;'/>"
        myHTMLTable += "    <col style='text-align:right;'/>"
        myHTMLTable += "    <col style='text-align:right;'/>"
        myHTMLTable += "    <col style='text-align:right;'/>"
        myHTMLTable += "    <col style='text-align:right;'/>"
        myHTMLTable += '    </colgroup>'
        myHTMLTable += '    <thead>'
        myHTMLTable += '    <tr>'
        myHTMLTable += "        <th style='text-align:left;'>Symbol</th>"
        myHTMLTable += "        <th style='text-align:right;' width=80>Win</th>"
        myHTMLTable += "        <th style='text-align:right;' width=80>Loss</th>"
        myHTMLTable += "        <th style='text-align:right;' width=80>Total</th>"
        myHTMLTable += "        <th style='text-align:right;' width=120>Winrate</th>"
        myHTMLTable += '    </tr>'
        myHTMLTable += '    </thead>'
        myHTMLTable += '    <tbody>'

        for tmp in data.index:
            symbol = tmp
            win = data.loc[tmp, 'win']
            loss = data.loc[tmp, 'loss']
            total = data.loc[tmp, 'total']
            Winrate = round((win / (win + loss)) * 100, 1)
            myHTMLTable += '    <tr>'
            myHTMLTable += f"        <td style='text-align:left;'>{symbol}</td>"
            myHTMLTable += f"        <td style='text-align:right;'>{win}</td>"
            myHTMLTable += f"        <td style='text-align:right;'>{loss}</td>"
            myHTMLTable += f"        <td style='text-align:right;'>{total}</td>"
            myHTMLTable += f"        <td style='text-align:right;'>{Winrate} %</td>"
            myHTMLTable += '    </tr>'
        myHTMLTable += '    </tbody>'
        myHTMLTable += '    </table>'

        return myHTMLTable

    def get_stragies_statistics(self):
        myCommand = (f"select symbol, strategy, win, loss, (win + loss) as total, winrate, profit, avg_days_trade  from main.strategies;")
        sql = text(myCommand)
        data = pd.read_sql_query(sql , engine, index_col='strategy')
    
        myHTMLTable = ""
        myHTMLTable += "    <table >"
        myHTMLTable += '    <colgroup>'
        myHTMLTable += "    <col style='text-align:left;'/>"
        myHTMLTable += "    <col style='text-align:right;'/>"
        myHTMLTable += "    <col style='text-align:right;'/>"
        myHTMLTable += "    <col style='text-align:right;'/>"
        #myHTMLTable += "    <col style='text-align:right;'/>"
        myHTMLTable += "    <col style='text-align:right;'/>"
        myHTMLTable += "    <col style='text-align:right;'/>"
        myHTMLTable += "    <col style='text-align:right;'/>"
        myHTMLTable += '    </colgroup>'
        myHTMLTable += '    <thead>'
        myHTMLTable += '    <tr>'
        myHTMLTable += "        <th style='text-align:left;' width=60>Symbol</th>"
        myHTMLTable += "        <th style='text-align:right;' width=100>Strategy</th>"
        myHTMLTable += "        <th style='text-align:right;' width=80>Win</th>"
        myHTMLTable += "        <th style='text-align:right;' width=80>Loss</th>"
        #myHTMLTable += "        <th style='text-align:right;' width=80>Total</th>"
        myHTMLTable += "        <th style='text-align:right;' width=120>Winrate</th>"
        myHTMLTable += "        <th style='text-align:right;' width=140>Profit</th>"
        myHTMLTable += "        <th style='text-align:right;' width=75>ø Days open</th>"
        myHTMLTable += '    </tr>'
        myHTMLTable += '    </thead>'
        myHTMLTable += '    <tbody>'   
        for tmp in data.index:
            symbol = data.loc[tmp, 'symbol']
            strategy = tmp
            win = data.loc[tmp, 'win']
            loss = data.loc[tmp, 'loss']
            total = data.loc[tmp, 'total']
            Winrate = data.loc[tmp, 'winrate']
            value = data.loc[tmp, 'profit']
            value = '${:,.2f}'.format(value)
            avg_days_trade = data.loc[tmp, 'avg_days_trade']
            myHTMLTable += '    <tr>'
            myHTMLTable += f"        <td style='text-align:left;'>{symbol}</td>"
            myHTMLTable += f"        <td style='text-align:right;'>{strategy}</td>"
            myHTMLTable += f"        <td style='text-align:right;'>{win}</td>"
            myHTMLTable += f"        <td style='text-align:right;'>{loss}</td>"
            #myHTMLTable += f"        <td style='text-align:right;'>{total}</td>"
            myHTMLTable += f"        <td style='text-align:right;'>{Winrate} %</td>"
            myHTMLTable += f"        <td style='text-align:right;'>{value}</td>"
            myHTMLTable += f"        <td style='text-align:right;'>{avg_days_trade} d</td>"
            myHTMLTable += '    </tr>'

        total_value = round(data['profit'].sum(),2)
        total_value = '${:,.2f}'.format(total_value)

        myHTMLTable += '    <tr>'
        myHTMLTable += f"        <td style='text-align:left;'><b>Total Profit</b></td>"
        myHTMLTable += f"        <td style='text-align:right;'></td>"
        myHTMLTable += f"        <td style='text-align:right;'></td>"
        myHTMLTable += f"        <td style='text-align:right;'></td>"
        myHTMLTable += f"        <td style='text-align:right;'></td>"
        myHTMLTable += f"        <td style='text-align:right;'></td>"
        myHTMLTable += f"        <td style='text-align:right;'><b>{total_value}</b></td>"
        myHTMLTable += f"        <td style='text-align:right;'></td>"
        myHTMLTable += '    </tr>'
        myHTMLTable += '    </tbody>'
        myHTMLTable += '    </table>' 

        return myHTMLTable

    def db_report_week(self, year, week, myCash):
        
        (returnsTable, returns_url,  monthly_url, yearly_url, drawdown_url, first_date, last_date) = self.create_stats()

        myReport = ""
        
        myHead = f"ETF Trend Trader Weekly Report {year} - {week}\n\n"
        short_message = f"Our performance since {first_date}.\n\n"
        post_type = "weekly"
        myReport += "<!--kg-card-begin: html-->"
        myReport += f"Our performance since {first_date}."
        myReport += "<!--kg-card-end: html-->"
        
        myReport += "<!--kg-card-begin: html-->"
        myReport += '<h3>Returns ETF Trend Trader</h3>'
        myReport += f"<figure><img src='{returns_url}' alt=''/></figure>"
        myReport += "<!--kg-card-end: html-->"

        myReport += '<!--kg-card-begin: html-->'
        myReport += f"<h3>KPI’s since {first_date}</h3>"
        myReport += f"{returnsTable}"
        myReport += '<!--kg-card-end: html-->\n'

        #######
        myValues = self.db_strategy_values()
        # add a new line to myValues with the total value of the portfolio
        new_record = pd.DataFrame([{'symbol':'cash', 'value':myCash}])
        myValues = pd.concat([myValues, new_record], ignore_index=True)

        fig = px.pie(myValues, values='value', names='symbol', title='Value Distribution')
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.write_image("./reports/strategy_values.png")
        strategy_values_url = self.ghost_file("./reports/strategy_values.png", f"strategy_values.png")
        myReport += "<!--kg-card-begin: html-->"
        myReport += '<h3>Strategy Value Distribution</h3>'
        myReport += f"<figure><img src='{strategy_values_url}' alt=''/></figure>"
        myReport += "<!--kg-card-end: html-->"

        myValues = self.db_get_winloss()
        fig = px.bar(myValues, x="symbol", y=["win", "loss"], title="Win / Loss per Symbol")
        #fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(barmode='group')
        fig.write_image("./reports/winloss.png")
        winloss_url = self.ghost_file("./reports/winloss.png", f"winloss.png") 
        myReport += "<!--kg-card-begin: html-->"
        myReport += '<h3>Win Loss Statistics</h3>'
        myReport += f"<figure><img src='{winloss_url}' alt=''/></figure>"
        myReport += "<!--kg-card-end: html-->" 

        mySymbols = self.get_symbol_statistics()
        myReport += '<!--kg-card-begin: html-->\n'
        myReport += '<h3>Symbol Statistics</h3>'
        myReport += f"{mySymbols}"
        myReport += '<!--kg-card-end: html-->\n'
        
        Strategy_Table = self.get_stragies_statistics()
        myReport += '<!--kg-card-begin: html-->\n'
        myReport += '<h3>Strategy Statistics</h3>'
        myReport += f"    {Strategy_Table}"
        myReport += '<!--kg-card-end: html-->\n'
        

        myReport += '<!--kg-card-begin: html-->\n'
        myReport += '<h3>Monthly Returns</h3>'
        myReport += f"<figure><img src='{monthly_url}' alt=''/></figure>"
        myReport += '<!--kg-card-end: html-->\n'

        myReport += '<!--kg-card-begin: html-->\n'
        myReport += '<h3>Yearly Returns</h3>'
        myReport += f"<figure><img src='{yearly_url}' alt=''/></figure>"
        myReport += '<!--kg-card-end: html-->\n'

        myReport += '<!--kg-card-begin: html-->\n'
        myReport += '<h3>Drawdown / Underwater Plot</h3>'
        myReport += f"<figure><img src='{drawdown_url}' alt=''/></figure>"
        myReport += '<!--kg-card-end: html-->\n'

        # Our Trades of the week
        myCommand = (f"select id, symbol, strategy, quantity, price, cur_winrate, cur_positions, round(cur_investment * 100,2) as invest_pcnt from main.Trades_open  where year = {year} and week = {week}")
        sql = text(myCommand)
        data = pd.read_sql_query(sql , engine, index_col='id')
        
        myReport += '<!--kg-card-begin: html-->'
        myReport += "    <h3>opened trades of this week</h3><p></p>"
        myReport += "    <table >"
        myReport += '    <colgroup>'
        myReport += "    <col style='text-align:left;'/>"
        myReport += "    <col style='text-align:left;'/>"
        myReport += "    <col style='text-align:right;'/>"
        myReport += "    <col style='text-align:right;'/>"
        myReport += "    <col style='text-align:right;'/>"
        myReport += "    <col style='text-align:right;'/>"
        myReport += "    <col style='text-align:right;'/>"
        myReport += '    </colgroup>'
        myReport += '    <thead>'
        myReport += '    <tr>'

        myReport += "        <th style='text-align:left;'>Symbol</th>"
        myReport += "        <th style='text-align:left;'>Strategy</th>"
        myReport += "        <th style='text-align:right;'>Quantity</th>"
        myReport += "        <th style='text-align:right;'>Price</th>"
        myReport += "        <th style='text-align:right;'>open Pos.</th>"
        myReport += "        <th style='text-align:right;'>Winrate</th>"
        myReport += "        <th style='text-align:right;'>Invest</th>"
        myReport += '    </tr>'
        myReport += '    </thead>'
        myReport += '    <tbody>'

        for tmp in data.index:
            symbol = data.loc[tmp, 'symbol']
            strategy = data.loc[tmp, 'strategy']
            quantity = data.loc[tmp, 'quantity']
            price = data.loc[tmp, 'price']
            winrate = data.loc[tmp, 'cur_winrate']
            positions = data.loc[tmp, 'cur_positions']
            percent = data.loc[tmp, 'invest_pcnt']

            
            myReport += '    <tr>'
            myReport += f"        <td style='text-align:left;'>{symbol}</td>"
            myReport += f"        <td style='text-align:right;'>{strategy}</td>"
            myReport += f"        <td style='text-align:right;'>{quantity}</td>"
            myReport += f"        <td style='text-align:right;'>{price} $</td>"
            myReport += f"        <td style='text-align:right;'>{positions}</td>"
            myReport += f"        <td style='text-align:right;'>{winrate} %</td>"
            myReport += f"        <td style='text-align:right;'>{percent} %</td>"
            myReport += '    </tr>'

        myReport += '    </tbody>'
        myReport += '    </table><p></p>'
        myReport += '<!--kg-card-end: html-->\n'

        myCommand = (f"select Trades_close.id, Trades_close.symbol as symbol, Trades_close.strategy as strategy , Trades_close.quantity as quantity, trades_open.date as open_date, trades_open.price as open_price, Trades_close.price as close_price, round(((trades_close.price - Trades_open.price) * trades_close.quantity),2) as profit, round((((trades_close.price / trades_open.price) - 1)*100),2) as profit_pcnt from main.Trades_close  inner join Trades_open on Trades_close.open_id = Trades_open.id where Trades_close.year = {year} and Trades_close.week = {week} ")
        sql = text(myCommand)
        data = pd.read_sql_query(sql , engine, index_col='id')
        
        myReport += '<!--kg-card-begin: html-->'
        myReport += "<h3>closed trades of this week</h3><p></p>"
        myReport += "    <table >"
        myReport += '    <colgroup>'
        myReport += "    <col style='text-align:left;'/>"
        myReport += "    <col style='text-align:left;'/>"
        myReport += "    <col style='text-align:right;'/>"
        #myReport += "    <col style='text-align:right;'/>"
        myReport += "    <col style='text-align:right;'/>"
        myReport += "    <col style='text-align:right;'/>"
        myReport += "    <col style='text-align:right;'/>"
        myReport += "    <col style='text-align:right;'/>"
        myReport += '    </colgroup>'
        myReport += '    <thead>'
        myReport += '    <tr>'

        myReport += "        <th style='text-align:left;'>Symbol</th>"
        myReport += "        <th style='text-align:left;'>Strategy<br>open Date</th>"
        myReport += "        <th style='text-align:right;'>Quantity</th>"
        #myReport += "        <th style='text-align:right;'>open Date</th>"
        myReport += "        <th style='text-align:right;'>open</th>"
        myReport += "        <th style='text-align:right;'>close</th>"
        myReport += "        <th style='text-align:right;'>Profit $</th>"
        myReport += "        <th style='text-align:right;'>Profit %</th>"
        myReport += '    </tr>'
        myReport += '    </thead>'
        myReport += '    <tbody>'

        for tmp in data.index:
            symbol = data.loc[tmp, 'symbol']
            strategy = data.loc[tmp, 'strategy']
            quantity = data.loc[tmp, 'quantity']
            open_date = data.loc[tmp, 'open_date']
            open_price = data.loc[tmp, 'open_price']
            close_price = data.loc[tmp, 'close_price']
            profit = data.loc[tmp, 'profit']
            profit_pcnt = data.loc[tmp, 'profit_pcnt']
            
            myReport += '    <tr>'
            myReport += f"        <td style='text-align:left;'>{symbol}</td>"
            myReport += f"        <td style='text-align:right;'>{strategy}<br>{open_date}</td>"
            myReport += f"        <td style='text-align:right;'>{quantity}</td>"
            #myReport += f"        <td style='text-align:right;'>{open_date}</td>"
            myReport += f"        <td style='text-align:right;'>{open_price} $</td>"
            myReport += f"        <td style='text-align:right;'>{close_price} $</td>"
            myReport += f"        <td style='text-align:right;'>{profit} $</td>"
            myReport += f"        <td style='text-align:right;'>{profit_pcnt} %</td>"
            myReport += '    </tr>'

        myReport += '    </tbody>'
        myReport += '    </table><p></p>'
        myReport += '<!--kg-card-end: html-->\n'


        myReport += '<!--kg-card-begin: html-->\n'
        myReport += '<h3>Additional Informations</h3>'
        ## Vergleich der Datenquellen
        myReport += '<!--kg-card-end: html-->\n'

        self.ghost_post(myReport, myHead, short_message, post_type=post_type)

    def db_report_trades_of_day(self, date):
        myCommand = (f"select id, symbol, strategy, quantity, price, cur_winrate, cur_positions, round(cur_investment * 100,2) as invest_pcnt from main.Trades_open  where date = '{date}'")
        sql = text(myCommand)
        data = pd.read_sql_query(sql , engine, index_col='id')
        
        myReport = ""
        myReport += '<!--kg-card-begin: html-->'
        myReport += "    <h3>Today we opened following trades.</h3><p> Please keep always in mind that the Invest is x% of your Cash, recalculated after every trade!</p><p></p>" 
        myReport += "    <table >"
        myReport += '    <colgroup>'
        myReport += "    <col style='text-align:left;'/>"
        myReport += "    <col style='text-align:left;'/>"
        myReport += "    <col style='text-align:right;'/>"
        myReport += "    <col style='text-align:right;'/>"
        myReport += "    <col style='text-align:right;'/>"
        myReport += "    <col style='text-align:right;'/>"
        myReport += "    <col style='text-align:right;'/>"
        myReport += '    </colgroup>'
        myReport += '    <thead>'
        myReport += '    <tr>'

        myReport += "        <th style='text-align:left;'>Symbol</th>"
        myReport += "        <th style='text-align:left;'>Strategy</th>"
        myReport += "        <th style='text-align:right;'>Quantity</th>"
        myReport += "        <th style='text-align:right;'>Price</th>"
        myReport += "        <th style='text-align:right;'>open Pos.</th>"
        myReport += "        <th style='text-align:right;'>Winrate</th>"
        myReport += "        <th style='text-align:right;'>Invest</th>"
        myReport += '    </tr>'
        myReport += '    </thead>'
        myReport += '    <tbody>'

        for tmp in data.index:
            symbol = data.loc[tmp, 'symbol']
            strategy = data.loc[tmp, 'strategy']
            quantity = data.loc[tmp, 'quantity']
            price = data.loc[tmp, 'price']
            price = '${:,.2f}'.format(price)
            winrate = data.loc[tmp, 'cur_winrate']
            positions = data.loc[tmp, 'cur_positions']
            percent = data.loc[tmp, 'invest_pcnt']

            
            myReport += '    <tr>'
            myReport += f"        <td style='text-align:left;'>{symbol}</td>"
            myReport += f"        <td style='text-align:right;'>{strategy}</td>"
            myReport += f"        <td style='text-align:right;'>{quantity}</td>"
            myReport += f"        <td style='text-align:right;'>{price} $</td>"
            myReport += f"        <td style='text-align:right;'>{positions}</td>"
            myReport += f"        <td style='text-align:right;'>{winrate} %</td>"
            myReport += f"        <td style='text-align:right;'>{percent} %</td>"
            myReport += '    </tr>'

        myReport += '    </tbody>'
        myReport += '    </table><p></p><p></p>'
        myReport += '<!--kg-card-end: html-->\n'



        myCommand = (f"select Trades_close.id, Trades_close.symbol as symbol, Trades_close.strategy as strategy , Trades_close.quantity as quantity, trades_open.date as open_date, trades_open.price as open_price, Trades_close.price as close_price, round(((trades_close.price - Trades_open.price) * trades_close.quantity),2) as profit, round((((trades_close.price / trades_open.price) - 1)*100),2) as profit_pcnt from main.Trades_close  inner join Trades_open on Trades_close.open_id = Trades_open.id where Trades_close.date = '{date}'")
        sql = text(myCommand)
        data = pd.read_sql_query(sql , engine, index_col='id')
        
        myReport += '<!--kg-card-begin: html-->'
        myReport += "<h3>Today we closed following trades.</h3><p></p>"
        myReport += "    <table >"
        myReport += '    <colgroup>'
        myReport += "    <col style='text-align:left;'/>"
        myReport += "    <col style='text-align:left;'/>"
        myReport += "    <col style='text-align:right;'/>"
        #myReport += "    <col style='text-align:right;'/>"
        myReport += "    <col style='text-align:right;'/>"
        myReport += "    <col style='text-align:right;'/>"
        myReport += "    <col style='text-align:right;'/>"
        myReport += "    <col style='text-align:right;'/>"
        myReport += '    </colgroup>'
        myReport += '    <thead>'
        myReport += '    <tr>'

        myReport += "        <th style='text-align:left;'>Symbol</th>"
        myReport += "        <th style='text-align:left;'>Strategy<br>open Date</th>"
        myReport += "        <th style='text-align:right;'>Quantity</th>"
        #myReport += "        <th style='text-align:right;'>open Date</th>"
        myReport += "        <th style='text-align:right;'>open</th>"
        myReport += "        <th style='text-align:right;'>close</th>"
        myReport += "        <th style='text-align:right;'>Profit $</th>"
        myReport += "        <th style='text-align:right;'>Profit %</th>"
        myReport += '    </tr>'
        myReport += '    </thead>'
        myReport += '    <tbody>'

        for tmp in data.index:
            symbol = data.loc[tmp, 'symbol']
            strategy = data.loc[tmp, 'strategy']
            quantity = data.loc[tmp, 'quantity']
            open_date = data.loc[tmp, 'open_date']
            open_price = data.loc[tmp, 'open_price']
            close_price = data.loc[tmp, 'close_price']
            profit = data.loc[tmp, 'profit']
            profit_pcnt = data.loc[tmp, 'profit_pcnt']
            
            myReport += '    <tr>'
            myReport += f"        <td style='text-align:left;'>{symbol}</td>"
            myReport += f"        <td style='text-align:right;'>{strategy}<br>{open_date}</td>"
            myReport += f"        <td style='text-align:right;'>{quantity}</td>"
            #myReport += f"        <td style='text-align:right;'>{open_date}</td>"
            myReport += f"        <td style='text-align:right;'>{open_price} $</td>"
            myReport += f"        <td style='text-align:right;'>{close_price} $</td>"
            myReport += f"        <td style='text-align:right;'>{profit} $</td>"
            myReport += f"        <td style='text-align:right;'>{profit_pcnt} %</td>"
            myReport += '    </tr>'

        myReport += '    </tbody>'
        myReport += '    </table><p></p><p></p>'
        myReport += '<!--kg-card-end: html-->\n'

        post_type = 'trades'
        short_message = f"Today's trades"
        myHead = f"Trades from {date}"
        self.ghost_post(myReport, myHead, short_message, post_type=post_type)

if __name__ == "__main__":
    if not IS_BACKTESTING:
        print("Running live strategy")
        ####
        # Run the strategy live
        ####


    else:
        print("Running backtesting strategy")
        ####
        # Backtest the strategy
        ####

    
        

        backtesting_start = datetime(2014, 1, 1)
        backtesting_end = datetime(2024, 4, 20) #datetime.now() - timedelta(days=3)

        etf.backtest(
            YahooDataBacktesting,
            backtesting_start,
            backtesting_end,
            show_plot=True,
            show_tearsheet=True,
            save_tearsheet=True,

        )



