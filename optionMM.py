#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 17 18:34:41 2024

@author: fredriklangbo
"""
#bæsj2
#PARAMETERS

#PARAMTERS
# Hvilken orderbook vi legger ordre i (Den vil automatisk hedge with the opposite contract)
# Fungerer antageligvis best om kun en av dem er True
MM_SELL_ORDER = True
MM_BUY_ORDER = False

SELL = "BTC-27DEC24"#"BTC-27JAN23" # Put the orders in ask
#SELL = "BTC-PERPETUAL"#"BTC-PERPETUAL" #Put the orders in bid

BUY = "BTC-12JAN24"#"BTC-PERPETUAL" #Put the orders in bid
crypto = "BTC"


enterprice = 4300#-30 nå håper vi å selge oss ut på 4600 ca
buyrange = 5
top_range = enterprice + buyrange
low_range = enterprice - buyrange

unload_qty = 4000 # the qty to sell before terminating the program
trade_qty = 100

wanted_diff = 0 # if a certain hedged should be maintained

margin = 0
max_margin = 0.7
initMargin = 0
localMargin = 0

# Option Margin in dollars
dOptionMargin = 50 # Our margin in dollar when calculating mmPrice
strike_interval = 5000 # what interval to calculate mmPrice at program init
future_upd_thshld = 15
max_dSize = 1000 # Maxium dollar size per order, can be adjusted to increase or lower init margin tolerance
my_order_book = {}

# Imports
import simplefix as fix
import time
import socket
import base64
import hashlib
import secrets
from datetime import datetime
import pandas as pd
import sys
import bisect
import traceback
import math
from openpyxl import load_workbook
import configparser

config = configparser.ConfigParser()
config.read('config.ini')  # Read the configuration file

# Retrieve the API keys securely
api_key = config['deribit']['api_key']
api_secret = config['deribit']['api_secret']

# Variables
inc = 2 # The increamenting value for each FIX message
quoteInc = 1
req_profit = 0


# Establishing a socket to send FIX messages through
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#s.connect(("193.58.254.1", 8025))
s.connect(("www.deribit.com", 9881))

def fix_tag(value):
    """Make a FIX tag value from string, bytes, or integer."""
    if sys.version_info[0] == 2:
        return bytes(value)

    if type(value) is bytes:
        return value

    if type(value) is str:
        return value.encode('ASCII')

    return str(value).encode('ASCII')
# important functions
def msgToStr(message):
    return str(message).split("'")[1].replace("\\x01", "|")

def m2s(msg):
    return str(msg).split("'")[1]

def m2i(msg):
    return int(str(msg).split("'")[1])

def m2f(msg):
    return float(str(msg).split("'")[1])



# Functions for effectivly handeling orders
def index(a, x):
    'Locate the leftmost value exactly equal to x'
    i = bisect.bisect_left(a, x)
    if i != len(a) and a[i] == x:
        return i
    #raise ValueError
    print("Could not locate value (ASK)")

def reverse_insort(a, x, lo=0, hi=None):
    """Insert item x in list a, and keep it reverse-sorted assuming a
    is reverse-sorted

    If x is already in a, insert it to the right of the rightmost x

    Optional args lo (default 0) and hi (default len(a)) bound the
    slice of a to be searched
    """
    if lo < 0:
        raise ValueError('lo must be non-negative')
    if hi is None:
        hi = len(a)
    while lo < hi:
        mid = (lo+hi)//2
        if x > a[mid]: hi = mid
        else: lo = mid+1
    a.insert(lo, x)

def indexBID(a, x):
    'Locate the leftmost value exactly equal to x'
    i = len(a) - bisect.bisect_right(a[::-1], x)
    if i != len(a) and a[i] == x:
        return i
    #raise ValueError
    print("Could not locate value (BID)")
    
    
def tcost(option1, option2, ul):
    if ul * 0.03 / 100 < abs(option1 * 0.125):
        trade1 = ul * 0.03 / 100
    else:
        trade1 = abs(option1) * 0.125

    if ul * 0.03 / 100 < abs(option2 * 0.125):
        trade2 = ul * 0.03 / 100
    else:
        trade2 = abs(option2) * 0.125

    if ul * 0.015 / 100 < abs(option1 * 0.125):
        delivery1 = ul * 0.015 / 100
    else:
        delivery1 = abs(option1) * 0.125

    if ul * 0.015 / 100 < abs(option2 * 0.125):
        delivery2 = ul * 0.015 / 100
    else:
        delivery2 = abs(option2) * 0.125

    return 0.00075*ul + trade1 + trade2 + delivery1 + delivery2

# This function uses the already existing orders in the market to calculate the 
# price of the opposite contract. If we pass an existing call into this function
# we will receive the potentially profitable price of a put

def calculateMarketMakerPrice(bidask,contract):
    contractName = contract.split("-")[0]+"-"+contract.split("-")[1]
    strike = contract.split("-")[2]
    futureAskPrice = order_book[contractName]["ask"]["price"][0]
    futureBidPrice = order_book[contractName]["bid"]["price"][0]
    price = order_book[contract][bidask]["price"][0]
    if bidask == "ask":
        if "-P" in contract: #if we are getting put and ask, we need to genereate price for a ask call
            mmPrice = (price*futureAskPrice + futureAskPrice - float(strike))/futureAskPrice
            print(mmPrice)
            mmPrice += (tcost(mmPrice*futureAskPrice,price*futureAskPrice,futureAskPrice) + dOptionMargin)/futureAskPrice
            print(mmPrice)
        if "-C" in contract: #if call
            mmPrice = (float(strike) + price*futureBidPrice - futureBidPrice)/futureBidPrice
            print(mmPrice)
            mmPrice += (tcost(mmPrice*futureBidPrice,price*futureBidPrice,futureBidPrice) + dOptionMargin)/futureBidPrice
            print(mmPrice)
    if bidask == "bid":
        if "-P" in contract: #if put
            mmPrice = (price*futureBidPrice + futureBidPrice - float(strike))/futureBidPrice
            print(mmPrice)
            mmPrice += -(tcost(mmPrice*futureBidPrice,price*futureBidPrice,futureBidPrice) + dOptionMargin)/futureBidPrice
            print(mmPrice)
        if "-C" in contract: #if call
            mmPrice = (float(strike) + price*futureAskPrice - futureAskPrice)/futureAskPrice
            print(mmPrice)
            mmPrice += -(tcost(mmPrice*futureAskPrice,price*futureAskPrice,futureAskPrice) + dOptionMargin)/futureAskPrice
            print(mmPrice)
            
    return mmPrice

def round_down(n):
    return math.floor(n / 0.0005) * 0.0005

def round_up(n):
    return math.ceil(n / 0.0005) * 0.0005
    
# FIX functions
    
#########################################################################################
def logIn(): # TO OPEN MORE CHANNELS TRY LOGGING IN WITH A NEW API KEY ON A NEW SOCKET
    ###################################################################################
    message = fix.FixMessage()

    nonce = secrets.token_urlsafe()
    encodedBytes = base64.b64encode(nonce.encode("utf-8"))
    encodedNonce = str(encodedBytes, "utf-8")
    timestamp = time.time_ns()
    raw_data = str(timestamp)[:-6] + "." + encodedNonce
    base_signature = raw_data + api_secret
    sha256 = hashlib.sha256(base_signature.encode('utf-8'))
    secret = sha256.hexdigest()
    secret = bytes.fromhex(secret)
    encodedBytes = base64.b64encode(secret)
    secret = str(encodedBytes, "utf-8")

    message.append_pair(8, "FIX.4.4")
    message.append_pair(35, "A")
    message.append_pair(49, "LSCMAS")
    message.append_pair(56, "DERIBITSERVER")
    message.append_pair(34, 1)
    message.append_pair(108, 10)
    message.append_pair(96, raw_data)
    message.append_pair(553, api_key)
    message.append_pair(554, secret)
    message.append_pair(9001, "Y")

    print(message.encode())
    s.sendall(message.encode())
    print(s.recv(4096))
    #return message

def heartbeat(): # heartbeat
    global inc
    message = fix.FixMessage()
    message.append_pair(8, "FIX.4.4")
    message.append_pair(35, "0")
    message.append_pair(34, inc) # this should be incremental
    message.append_pair(49, "LSCMAS")
    #message.append_pair(52, "20211228-18:28:27.000")
    message.append_pair(56, "DERIBITSERVER")
    message.append_pair(11, "TESTing") #Unique ID
    inc +=1
    #s.sendall(message.encode())
    return message

def newOrder(symbol, orderType, price, qty, side, marketMaker="no"): # I need to be able to identify the Orders ID, when getting a confirmation. Will the uniquie ID be returned?
    if side.lower() == "buy":
        side = "1"
    elif side.lower() == "sell":
        side = "2"
        
    global inc
    message = fix.FixMessage()
    message.append_pair(8, "FIX.4.4")
    message.append_pair(35, "D")
    message.append_pair(34, inc) # this should be incremental
    message.append_pair(49, "LSCMAS")
    #message.append_pair(52, "20211228-18:28:27.000")
    message.append_pair(56, "DERIBITSERVER")
    message.append_pair(11, "TESTing") #Unique ID
    message.append_pair(38, qty) # The desired quanity. OPS! 10USD = 1qty, uncertain of how options are handeld.
    message.append_pair(40, orderType) # Order type. Valid values: 1 = Market, 2 = Limit. (default Limit)
    message.append_pair(44, price) # The price of the limit order, should set to 0 if market
    message.append_pair(54, side) # Buy(1) or sell(2)
    message.append_pair(55, symbol) # The instrument to trade
    if marketMaker.lower() == "mm": # check if the trades is supposed to be a market trade
        message.append_pair(18, "6") # trades will not be posted if they will filled as taker
    inc +=1
    #s.sendall(message.encode())
    return message

def massCancel(symbol="none"): # I need to be able to identify the Orders ID, when getting a confirmation. Will the uniquie ID be returned?
    global inc
    message = fix.FixMessage()
    message.append_pair(8, "FIX.4.4")
    message.append_pair(35, "q")
    message.append_pair(34, inc) # this should be incremental
    message.append_pair(49, "LSCMAS")
    #message.append_pair(52, "20211228-18:28:27.000")
    message.append_pair(56, "DERIBITSERVER")
    message.append_pair(11, "TESTing") #Unique ID
    if symbol == "none":
        message.append_pair(530, 7) # Specifies the type of cancellation requested. Supported values: 7 (all orders), 
    # 5 (orders by security type), 1 (orders by symbol), 10 (orders by DeribitLabel).
    else: 
        message.append_pair(530, 1)
        message.append_pair(55, symbol)

    inc +=1
    #s.sendall(message.encode())
    return message

def subscribeMarketData(subArray):
    global inc
    message = fix.FixMessage()
    message.append_pair(8, "FIX.4.4")
    message.append_pair(35, "V")
    message.append_pair(34, inc)
    message.append_pair(49, "arbBOIi1997")
    message.append_pair(52, "20211228-18:28:27.000") # This is probably not needed
    message.append_pair(56, "DERIBITSERVER")

    message.append_pair(262, "TESTing") # unquie ID
    message.append_pair(263, 1) # SubscriptionRequestType 0 = Snapshot, 1 = Snapshot + Subscribe, 2 = Unsubscribe
    message.append_pair(264, 0) # Marketdepth
    message.append_pair(265, 1) # MDUpdateType 0 = full refresh, 1 = incremental updates

    message.append_pair(267, 2) # NoMdEntryTypes group. =2 since we request bid and ask
    #message.append_pair(269, 2) 
    message.append_pair(269, 0) # This is Bid
    message.append_pair(269, 1) # This is Ask
    
    #message.append_pair(146, 1)
    #message.append_pair(55, "BTC-25MAR22-40000-C") #"BTC-24JUN22-45000-P" "BTC-PERPETUAL"
    
    options =[]
    futures =[]
    sublist = []
    #The entire option chain + futures is included here. 
    for instrument in subArray: 
        lastTwo = m2s(instrument)[-2:]
        if lastTwo == "-C" or lastTwo == "-P":
            options.append(instrument)
        else:
            if "PERPETUAL" not in m2s(instrument):
                if "VIX" not in m2s(instrument):
                    if "INDEX" not in m2s(instrument):
                        if "ETH" not in m2s(instrument):
                            futures.append(instrument)
                            sublist.append(instrument) 
                        
    for option in options:  
        option_date = m2s(option).split("-")[0] +"-" + m2s(option).split("-")[1]
        for future in futures:
            future_date = m2s(future)
            if future_date == option_date:
                sublist.append(option) 
    
    message.append_pair(146, len(sublist)) # Number of symbols requested. Necessary if more than 1 Symbol requested
  
    for instrument in sublist:
        message.append_pair(55, m2s(instrument))
        print("subbing to: ", m2s(instrument))
    inc +=1
    
    return message

def userData():
    global inc

    message = fix.FixMessage()
    message.append_pair(8, "FIX.4.4")
    message.append_pair(35, "BE")
    message.append_pair(34, inc)
    message.append_pair(49, "LSCMAS")
    message.append_pair(52, "20211228-18:28:27.000") # This is probably not needed
    message.append_pair(56, "DERIBITSERVER")

    message.append_pair(923, "LSCMAS") #The request ID
    message.append_pair(924, 4) # Should be equal to 4 (Request individual user status), only UserRequestType=4 supported for now
    message.append_pair(553, api_key) # API authenticated 'Access Key', user can request only own info, should be the same as for previous LOGON(A)
    message.append_pair(15, crypto)
    
    inc +=1
    
    return message
# def positionTracker():
#     global inc
#     message = fix.FixMessage()
#     message.append_pair(8, "FIX.4.4")
#     message.append_pair(35, "BE")
#     message.append_pair(34, inc)
#     message.append_pair(49, "LSCMAS")
#     message.append_pair(52, "20211228-18:28:27.000") # This is probably not needed
#     message.append_pair(56, "DERIBITSERVER")

#     message.append_pair(710, "TESTing") #The request ID
#     message.append_pair(724, 0) # 0 = Positions (currently)
#     message.append_pair(263, 0) # 0=Receive snapshot, 1=subscribe, 2=unsubscribe

#     inc +=1
    
#     return message

def posistionRequest():
    global inc

    message = fix.FixMessage()
    message.append_pair(8, "FIX.4.4")
    message.append_pair(35, "AN")
    message.append_pair(34, inc)
    message.append_pair(49, "LSCMAS")
    message.append_pair(52, "20211228-18:28:27.000") # This is probably not needed
    message.append_pair(56, "DERIBITSERVER")

    message.append_pair(710, "TESTing") #The request ID
    message.append_pair(724, 0) # 0 = Positions (currently)
    message.append_pair(263, 0) # 0=Receive snapshot, 1=subscribe, 2=unsubscribe

    inc +=1
    
    return message

def securityListRequest():
    global inc
    message = fix.FixMessage()
    # Header
    message.append_pair(8, "FIX.4.4")
    message.append_pair(35, "x")
    message.append_pair(34, inc)
    message.append_pair(49, "arbBOIi1997")
    message.append_pair(52, "20211228-18:28:27.000") # This is probably not needed
    message.append_pair(56, "DERIBITSERVER")

    message.append_pair(320, "TESTing") # unquie ID
    message.append_pair(559, 4) # 4 = All
    message.append_pair(263, 0) # SubscriptionRequestType 0 = Snapshot, 1 = Snapshot + Subscribe, 2 = Unsubscribe
    
    inc +=1
    
    return message

def massQuote(massQuoteList):
    global inc
    global quoteInc
    message = fix.FixMessage()
    
    numberOfQuotes = len(massQuoteList)
    # Header
    message.append_pair(8, "FIX.4.4")
    message.append_pair(35, "i")
    message.append_pair(34, inc)
    message.append_pair(49, "arbBOIi1997")
    message.append_pair(52, "20211228-18:28:27.000") # This is probably not needed
    message.append_pair(56, "DERIBITSERVER")

    message.append_pair(117, "ID" + str(quoteInc)) 
    message.append_pair(9019, "MMgroup1") # Hvis vi skal ha flere bids på en enkelt opsjon må vi ha en ny MMPGroup
    message.append_pair(296, 1) # number of quotesets
    message.append_pair(302, str(quoteInc)) # this is the ID we refer to when we use quoteCancel
    message.append_pair(304, numberOfQuotes)
    message.append_pair(295, numberOfQuotes)
    idInc = 1
    for quote in massQuoteList:
        
        message.append_pair(299, idInc) # number of quotesets
        message.append_pair(55, quote[0]) # this is the ID we refer to when we use quoteCancel
        if quote[3] == "sell":
            if quote[1] != "null":
                message.append_pair(133, quote[1])
            if quote[2] != "null": 
                message.append_pair(135, quote[2])
        else:
            if quote[1] != "null":
                message.append_pair(132, quote[1])
            if quote[2] != "null":
                message.append_pair(134, quote[2])
        idInc +=1
    quoteInc += 1
    inc +=1
    
    return message

# Her lager vi en funksjon for å sjekke om vi allerede har en ordre ute i markedet
# for å unngå at vi bruker vår egen ordre til å regne market maker prisen
def notMatchingOrder(instrumetName, bidask):
    try:
        if my_order_book[instrumetName][bidask]["price"][0] == order_book[instrumetName][bidask]["price"][0]:
            return False
        else: 
            return True
    except:
        return True

def bestOrder(orderName, bidask, calc_price):
    if bidask == "bid":
        try:
            if order_book[orderName][bidask]["price"][0] < calc_price:
                return True
            else:
                return False
        except:
            return True
    else:
        try:
            if order_book[orderName][bidask]["price"][0] > calc_price:
                return True
            else:
                return False
        except:
            return True

# Vi lager en funksjon for å legge ordre i tradingqueuen. 
def addToTradingQueue(instrumetName, bidask, updateType=1): # updatetype 1 = both price and vol, 2 = just vol
    global localMargin
    if order_book[instrumetName][bidask]["mmPrice"]:     # check om det finnes en mmPrice, slik at vi ikke havner utenfor 5000 dollar intervallet

        split_name = instrumetName.split("-")
        if instrumetName[-1:] == "P": # name of the order we are executing 
            orderName = instrumetName[:-1]+"C"
        else: 
            orderName = instrumetName[:-1]+"P"
        print(orderName)
        ulPrice = order_book[split_name[0]+"-"+split_name[1]]["ask"]["price"][0] # price of underlying
        qty = min(max_dSize, ulPrice*order_book[instrumetName][bidask]["price"][0]*order_book[instrumetName][bidask]["volume"][0])
        qty = round_down(qty/((ulPrice*order_book[instrumetName][bidask]["price"][0]))) # make qty denominated in bitcoin
        print(qty)
        # check margin krav
        if max_margin > initMargin and max_margin > localMargin:
            # check at ordren vi hedger med ikke er vår egen
            print("d1")
            if notMatchingOrder(instrumetName, bidask):
                print("d2")
                if bidask == "bid":
                    # calculates price for our new order using the hedgeing instrument(instrument name)
                    calc_price = round_down(calculateMarketMakerPrice("bid", instrumetName))
                    if bestOrder(orderName, "bid", calc_price): # check if our order is best priced OR there are no other quotes in the orderbook

                        calc_price = order_book[orderName]["bid"]["price"][0] + 0.0005
                                                            
                        print(calc_price)
                        #oppdater my_order_book med den nye ordren
                        if updateType == 2:
                            #add to queue
                            trading_queue.append([orderName, "null", qty, "buy"])
                        else:
                            #add to queue
                            trading_queue.append([orderName, calc_price, qty, "buy"])
                        
                        #increase local margin
                        if not my_order_book[orderName]["ask"]: # there is no entry in my_order_book, meaning this is a new order -> add margin
                            localMargin += qty*0.15
                        else: # order already exists in my orderbook
                            if my_order_book[orderName]["ask"]["volume"] != qty: # we check if there is a change in quantity, if so update the local margin
                                localMargin -= my_order_book[orderName]["ask"]["volume"]*0.15
                                localMargin += qty*0.15
                                
                        # add the new order to my_order_book
                        my_order_book[orderName]["bid"] = {"price":calc_price,"volume":qty}

                                
                    
                if bidask == "ask":
                    # calculates price for our new order using the hedgeing instrument(instrument name)
                    calc_price = round_up(calculateMarketMakerPrice("ask", instrumetName))
                    
                    if bestOrder(orderName, "ask", calc_price): # check if our order is best priced OR there are no other quotes in the orderbook

                        calc_price = order_book[orderName]["ask"]["price"][0] - 0.0005
                    
                        
                        #oppdater my_order_book med den nye ordren
                        if updateType == 2:
                            #add to queue
                            trading_queue.append([orderName, "null", qty, "sell"])
                        else:
                            #add to queue
                            trading_queue.append([orderName, calc_price, qty, "sell"])
                        # OLD : add to queue
                        # OLD : trading_queue.append([orderName, calc_price, qty, "sell"])
                        
                        #increase local margin
                        if not my_order_book[orderName]["bid"]:
                            localMargin += qty*0.15
                        else: # order already exists in my orderbook
                            if my_order_book[orderName]["bid"]["volume"] != qty: # we check if there is a change in quantity, if so update the local margin
                                localMargin -= my_order_book[orderName]["bid"]["volume"]*0.15
                                localMargin += qty*0.15
                            
                        #oppdater my_order_book med den nye ordren
                        my_order_book[orderName]["ask"] = {"price":calc_price,"volume":qty}
            
        

logIn()

s.sendall(securityListRequest().encode())

#msg = msgToStr(s.recv(400096))
#print(len(msg.split("55")))
heartbeat_count=0
order_book = {}
order_book2 = {}
c = 1
parser = fix.FixParser()
ARB_name=""
current_order = 0

unwind_order = 0
swap_order = 0
old_diff = 0

api_credit = 50000
trading_queue = [] # pass list [instrument_name, price, qty, side(buy/sell)]
timer = time.perf_counter()

while unload_qty > trade_qty:
    if api_credit > 5000:
        if trading_queue:
            #newOrder(symbol, orderType, price, qty, side, "mm")
            s.sendall(massQuote(trading_queue).encode())
            #newOrder(trading_queue[0][0], 2, trading_queue[0][1], trading_queue[0][2], trading_queue[0][3], "mm")
            api_credit -= 500
            trading_queue = []
            if len(trading_queue) > 200:
                print("WARING! LARGE TRADING QUEUE OF: ", len(trading_queue))
    if api_credit < 50000:
        current_time = time.perf_counter()
        api_credit += (current_time-timer)*10000
        api_credit = min(api_credit,50000)
        timer = time.perf_counter()

    try:
        buf = s.recv(4096)
    except:
        pass
    parser.append_buffer(buf)
    
    while True:
        try:
            msg = parser.get_message()
            #print(msg.count(55))

        except:
            pass
        if msg is None:
            break
        #else:
        #print(str(msg))
        if str(msg.get(35)).split("'")[1] == "y":
            subArray = []
            for i in range(1,int(msg.get(146))):
                #print(m2s(msg.get(167,i)))
                if m2s(msg.get(167,i)) == "FUT" or m2s(msg.get(167,i)) == "OPT":
                    subArray.append(msg.get(55,i))
            print("Subscribing!")
            s.sendall(subscribeMarketData(subArray).encode())
            
        if m2s(msg.get(35)) == "X":
            #print(str(msg))
            #for t,v in msg.pairs: # DO THIS TO AVOID A LOT OF LOOPS
            for t,v in msg.pairs: # Loop thorugh the msg to avoid multiple loops with .get function
            # since the msg is structured in a certain way we know instrument name is always first
            # then the type, price and volume is listed in turn for each order in the FIX msg
            # type and price is saved and used when t = volume tag since vol is the last value in the order 
            # based on the type the order is approriatly handled
                name_tag = fix_tag(55)  
                type_tag = fix_tag(279) # tag determining new, change or del
                bidask_tag = fix_tag(269)
                price_tag = fix_tag(270) 
                vol_tag = fix_tag(271)
                if t == name_tag: instrumetName = m2s(v)
                if t == type_tag: orderType = m2s(v)
                if t == bidask_tag: bidask = m2s(v)
                if t == price_tag: price = m2f(v)
                if t == vol_tag:
                    vol = m2f(v)

                    # now that all the values are retrived we process the order
                    # In this case we want to buy monthly contract and sell perp
                    # In that case we should react to changes in the monthly ask and perp bid
                
                    if bidask == "0": # BIDs
                        # this is used to check if the toplistinig changed later
                        top_listing = order_book[instrumetName]["bid"]["price"][0]
                        top_listing_vol = order_book[instrumetName]["bid"]["volume"][0]

                        if orderType == "0": # New orders
                            #Check for arb first, then append to order_book
                            top_listing = order_book[instrumetName]["bid"]["price"][0]
                                        
                            reverse_insort(order_book[instrumetName]["bid"]["price"], price)
                            idx = indexBID(order_book[instrumetName]["bid"]["price"], price) # find the pos of the price
                            order_book[instrumetName]["bid"]["volume"].insert(idx,vol)
                            
                        
                        if orderType == "1": # Change
                            order_book[instrumetName]["bid"]["volume"][indexBID(order_book[instrumetName]["bid"]["price"], price)] = vol
                        if orderType == "2": # Delete
                            # if the idx = 0 we know its removing the top listing! This can be used to dertemine when to change orders.
                            
                            idx = indexBID(order_book[instrumetName]["bid"]["price"], price)
                            del order_book[instrumetName]["bid"]["price"][idx]
                            del order_book[instrumetName]["bid"]["volume"][idx]
                        
                        if top_listing == order_book[instrumetName]["bid"]["price"][0] and top_listing_vol != order_book[instrumetName]["bid"]["volume"][0]:
                            # There was a change in the top listings volume, so we need to adjust the size of our posistion
                            if instrumetName[-1:] == "P": # Put update
                                # check if there is a position in my order book to change at all:
                                if my_order_book[instrumetName[:-1]+"C"]["bid"]: 
                                    # We have a order in the market affected by the change in volume
                                    # cahnge current order OR delete and make a new one
                                    addToTradingQueue(instrumetName, "bid", 2)
                                    
                        # there has been a change in the top listing price
                        if top_listing != order_book[instrumetName]["bid"]["price"][0]: 
                            split_name = instrumetName.split("-")
                            if len(split_name) > 2: # Options
                                print("adding to queue")
                                #addToTradingQueue()
                                addToTradingQueue(instrumetName, "bid")
                            else: # Futures
                                # Check that the futures prices have moved by more than 10 to update our orders 
                                if abs(order_book[instrumetName]["bid"]["spotChange"]-order_book[instrumetName]["bid"]["price"][0]) > future_upd_thshld:
                                    # Mass Cancel
                                    # Remove "old" orders from the trading queue, so the orders dont double up
                                    ticker = instrumetName.split("-")[0] + "-" + instrumetName.split("-")[1] 
                                    trading_queue = [item for item in trading_queue if item[0].split("-")[0] + "-" + item[0].split("-")[1] != ticker]
                                    # Calculate price for entire option chain
                                    # Get all the instrument names with the same expiration date
                                    instrument_list = [key for key in list(my_order_book.keys()) if ticker in key]
                                    #massQuoteList = []
                                    
                                    for instrument in instrument_list:
                                        # instrument is the full name of the insturment i.e "BTC-1MAR24-57000-P" ops! can be future too!
                                        split_name2 = instrument.split("-")
                                        if len(split_name2) > 2: # Options
                                            if split_name2[3] == "P":
                                                if my_order_book[instrument]["ask"]: # check if there is a existing outstanding ask order for this instrument
                                                    # calculates the new price
                                                    newPrice = calculateMarketMakerPrice("ask", instrument[:-1]+"C") # flip to call in order to use the calc function
                                                    # calculate quanity based on quanity to hedged option
                                                    qty = min(max_dSize, price*order_book[instrument[:-1]+"C"]["ask"]["price"][0]*order_book[instrument[:-1]+"C"]["ask"]["volume"][0])
                                                    qty = round_down(qty/((price*order_book[instrument[:-1]+"C"]["ask"]["price"][0]))) # make qty denominated in bitcoin 
                                                    # append to massquote list
                                                    trading_queue.append([instrument, newPrice, qty, "sell"])
                                                    # update my orderbook
                                                    my_order_book[instrument]["ask"] = {"price":newPrice,"volume":qty}
                                            if split_name2[3] == "C":
                                                if my_order_book[instrument]["bid"]: # check if there is a existing outstanding bid order for this instrument
                                                    newPrice = calculateMarketMakerPrice("bid", instrument[:-1]+"P") # flip to put in order to use the calc function
                                                                                                        
                                                    qty = min(max_dSize, price*order_book[instrument[:-1]+"P"]["bid"]["price"][0]*order_book[instrument[:-1]+"P"]["bid"]["volume"][0])
                                                    qty = round_down(qty/((price*order_book[instrument[:-1]+"P"]["bid"]["price"][0]))) # make qty denominated in bitcoin
                                                    # append to massquote list
                                                    trading_queue.append([instrument, newPrice, qty, "buy"])
                                                    # update my orderbook
                                                    my_order_book[instrument]["bid"] = {"price":newPrice,"volume":qty}
                                                    
                                        
                                    # Mass quoute
                                    # Now the massQuote list should be filled up
                                    print(massQuoteList)
                                    # Now either loop thorugh all the orders adding them to trading queue or pass the list to a mass qute function
                            
                            
                            
                    if bidask == "1": # ASKS
                        top_listing = order_book[instrumetName]["ask"]["price"][0]
                        
                        if orderType == "0": # New orders
                                    
                            bisect.insort(order_book[instrumetName]["ask"]["price"], price)
                            idx = index(order_book[instrumetName]["ask"]["price"], price) # find the pos of the price
                            order_book[instrumetName]["ask"]["volume"].insert(idx,vol)
                        if orderType == "1": # Change
                            order_book[instrumetName]["ask"]["volume"][index(order_book[instrumetName]["ask"]["price"], price)] = vol
                        if orderType == "2": # Delete
                        
                            # Update when ask perp is deleted, remember to check for top order being deleted
                            
                            idx = index(order_book[instrumetName]["ask"]["price"], price)
                            del order_book[instrumetName]["ask"]["price"][idx]
                            del order_book[instrumetName]["ask"]["volume"][idx]
                            
                        #checkinig if there has been a change in the top listing, if so calc premium and act acordingly
                        #this will add sell limit orders to the unwind order book 
                        if top_listing !=  order_book[instrumetName]["ask"]["price"][0]: 
                            print("no functionality added here yet")
            #print(order_book)
            #exit
        #else:
            #print(msg)
        if m2s(msg.get(35)) == "8":
            status = m2s(msg.get(39))
            symbol = m2s(msg.get(55))
            if symbol == SELL and MM_SELL_ORDER == True:
                if status == "1" or status == "2":
                    print(msg)
                    qty = m2f(msg.get(32))
                    #spread = order_book[crypto+"-PERPETUAL"]["ask"]["price"][0] - order_book[crypto+"-PERPETUAL"]["bid"]["price"][0]
                    unload_qty -= qty
                    s.sendall(newOrder(BUY, 1, 0, qty, "buy").encode())
                    
            if symbol == BUY and MM_BUY_ORDER == True:
                if status == "1" or status == "2":
                    print(msg)
                    qty = m2f(msg.get(32))
                    #spread = order_book[crypto+"-PERPETUAL"]["ask"]["price"][0] - order_book[crypto+"-PERPETUAL"]["bid"]["price"][0]
                    unload_qty -= qty
                    s.sendall(newOrder(SELL, 1, 0, qty, "sell").encode())
                    #print("Placeing market order as hedge, until order management is implementetd. The spread is: ", spread)


        if str(msg.get(35)).split("'")[1] == "0": # Heartbeat to maintain the connection
            s.sendall(heartbeat().encode())
            heartbeat_count +=1

            s.sendall(userData().encode())
            #s.sendall(positionTracker().encode())
            #print("msg")
            s.sendall(posistionRequest().encode())
            if heartbeat_count>60*6:
                print("1 hour!")
                heartbeat_count=0
                
        if str(msg.get(35)).split("'")[1] == "BF": # user info like equity, margin, pnl etc
            MaintenanceMargin = m2f(msg.get(100004))
            initMargin = m2f(msg.get(100003))
            
            Equity = m2f(msg.get(100001))
            initMargin = initMargin/Equity
            margin = MaintenanceMargin/Equity
            #print(margin)
            if margin > max_margin:
                s.sendall(massCancel().encode())
                exit
            #print(margin)
        if str(msg.get(35)).split("'")[1] == "AP": # print pos report
            #print(msg)    
            #pos_val = msg.get(#num)
            date_contract_qty = 0
            for t,v in msg.pairs: # Loop thorugh the msg to avoid multiple loops with .get function
                long_tag = fix_tag(704) # tag determining new, change or del
                short_tag = fix_tag(705)
                name_tag = fix_tag(55)  
                if t == long_tag: longqty = m2f(v)
                if t == short_tag: shortqty = m2f(v)
                if t == name_tag: 
                    instrumetName = m2s(v)
                    if "PERPETUAL" in instrumetName:
                        perpetual_qty = longqty - shortqty
                    else:
                        date_contract_qty = date_contract_qty + longqty - shortqty

                              
            # GET THE CURRENT POSITION SIZE OF THE CONTRACT AND UPDATE A VALUE TO TRACK THE SIZE, THEN USE THAT VALUE TO DETERMINE IF THERE SHOULD BE
            # MORE TRADES AND IF THE SIZE OF THE TRADE SHOULD BE REDUCED (IF ITS LESS THAN THE ORIGINAL NUMBER)
            
        if str(msg.get(35)).split("'")[1] == "5": # If logout is sent print the msg
            print(msg)
                
        if m2s(msg.get(35)) == "W": # Recive snapshot
            bidPrice = []
            bidVol = []
            askPrice = []
            askVol = []
            tag = fix_tag(269)  
            val = fix_tag(1)
            tag1 = fix_tag(270) 
            tag2 = fix_tag(271) 
            instrumetName = m2s(msg.get(55))
            bid=True
            for t,v in msg.pairs:
                if t == tag: 
                    if v==val:
                        bid = False

                if t == tag1: #price
                    #c+=1
                    if bid: # first half append to bid
                        bidPrice.append(m2f(v))
                    else: # second half append ask
                        askPrice.append(m2f(v))
                if t == tag2: #vol
                    #c+=1
                    if bid: # first half append to bid
                        bidVol.append(m2f(v))
                    else: # second half append ask
                        askVol.append(m2f(v))
            if len(bidPrice) >0:
                spotChangeBid = bidPrice[0]
            else:
                spotChangeBid = 0
            if len(askPrice) >0:
                spotChangeAsk = askPrice[0]
            else:
                spotChangeAsk= 0
            #order_book[instrumetName] = {"ask":{"price":askPrice,"volume":askVol},"bid":{"price":bidPrice,"volume":bidVol}}
            order_book[instrumetName] = {"ask":{"price":askPrice,"volume":askVol,"spotChange":spotChangeAsk},"bid":{"price":bidPrice,"volume":bidVol,"spotChange":spotChangeBid}}
            my_order_book[instrumetName] = {"ask":{},"bid":{}}

            for contract in order_book:
                splitcontract = contract.split("-")
                if len(splitcontract) > 2:
                    fut_ask = order_book[splitcontract[0]+"-"+splitcontract[1]]["ask"]["price"][0]
                    if float(splitcontract[2]) < fut_ask + strike_interval and float(splitcontract[2]) > fut_ask - strike_interval:
                        try:
                            order_book[contract]["ask"]["mmPrice"] = calculateMarketMakerPrice("ask", contract)
                        except:
                            order_book[contract]["ask"]["mmPrice"] = 0
                        try:
                            order_book[contract]["bid"]["mmPrice"] = calculateMarketMakerPrice("bid", contract)
                        except:
                            order_book[contract]["ask"]["mmPrice"] = 0


            #print(t2-t1)
        if m2s(msg.get(35)) == "3": # ERROR / REJECTION
            if "rate_limit" in m2s(msg.get(58)):
                print("RATE LIMIT EEXCEEDED")
                logIn()
            
        # Trying to locate too many request 
        if m2s(msg.get(35)) != "8" and m2s(msg.get(35)) != "r" and m2s(msg.get(35)) != "W" and m2s(msg.get(35)) != "5" and m2s(msg.get(35)) != "AP" and m2s(msg.get(35)) != "BF" and m2s(msg.get(35)) != "0" and m2s(msg.get(35)) != "X":
           print("=============")
           print(m2s(msg.get(35))) 
           print(msg)



massCancel()
print("Shutting down")
time.sleep(1)












