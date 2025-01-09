import simplefix
import base64
import hashlib
import secrets
import socket
import time
import datetime
import random
from threading import Thread

instrument_name = "BTC_USDT-PERPETUAL"
option_list = ["BTC-17JAN25", "BTC-31JAN25"]


def calculate_weighted_price(prices, volumes, target_volume):
    """
    Calculate weighted average price for a given target volume
    Returns (weighted_price, achieved_volume)
    """
    total_cost = 0
    total_volume = 0
    
    for price, volume in zip(prices, volumes):
        if total_volume >= target_volume:
            break
        usable_volume = min(volume, target_volume - total_volume)
        total_cost += price * usable_volume
        total_volume += usable_volume
        
    if total_volume == 0:
        return None, 0
        
    return total_cost / total_volume, total_volume

def get_option_details(symbol):
    """
    Parse option symbol to extract strike and type
    Returns (expiry, strike, type)
    """
    parts = symbol.split('-')
    if len(parts) != 4:
        return None, None, None

    expiry = parts[1]
    strike = parts[2]
    option_type = parts[3]

    return expiry, strike, option_type

class OrderManager:
    def __init__(self):
        self.open_orders = {}  # {order_id: order_details}
        self.positions = {}    # {symbol: position_size}
        self.quotes = {}       # {symbol: quote_details}
        self.hedge_orders = {} # Track hedge orders for each main order
        
    def track_order(self, order_id, symbol, side, price, quantity, hedge_symbols=None):
        """
        Track a new order with associated hedges
        hedge_symbols: dict of hedge instruments and their ratios
        """
        self.open_orders[order_id] = {
            'symbol': symbol,
            'side': side,
            'price': price,
            'quantity': quantity,
            'status': 'ACTIVE',
            'hedge_symbols': hedge_symbols or {},
            'hedge_orders': []
        }
    def update_order(self, order_id, new_status, filled_quantity=0):
        """Update order status and position"""
        if order_id in self.open_orders:
            order = self.open_orders[order_id]
            order['status'] = new_status
            
            if filled_quantity > 0:
                symbol = order['symbol']
                side_multiplier = 1 if order['side'] == '1' else -1  # 1 for buy, -1 for sell
                self.positions[symbol] = self.positions.get(symbol, 0) + (filled_quantity * side_multiplier)
                
    def add_quote(self, symbol, quote_id, bid_price, bid_size, ask_price, ask_size, 
             bid_hedge_ratios, ask_hedge_ratios, bid_hedge_symbols, ask_hedge_symbols):
        """Track a new or updated quote with hedge symbols"""
        self.quotes[symbol] = {
            'quote_id': quote_id,
            'bid_price': bid_price,
            'bid_size': bid_size,
            'ask_price': ask_price,
            'ask_size': ask_size,
            'bid_hedge_ratios': bid_hedge_ratios,
            'ask_hedge_ratios': ask_hedge_ratios,
            'bid_hedge_symbols': bid_hedge_symbols,
            'ask_hedge_symbols': ask_hedge_symbols,
            'timestamp': time.time()
        }
        
    def remove_quote(self, symbol):
        """Remove a quote from tracking"""
        self.quotes.pop(symbol, None)
        
    def add_fill_from_quote(self, symbol, side, price, quantity):
        """Convert a quote execution into a tracked order"""
        # Get the relevant quote
        quote = self.quotes.get(symbol)
        if not quote:
            print(f"Warning: Fill for symbol {symbol} but no quote found")
            return None
            
        # Get hedge ratios based on which side was hit
        hedge_ratios = quote['bid_hedge_ratios'] if side == '1' else quote['ask_hedge_ratios']
        
        # Generate order ID
        order_id = f"quote_fill_{int(time.time()*1000)}" + hex(random.getrandbits(32))
        
        # Track the order
        self.open_orders[order_id] = {
            'symbol': symbol,
            'side': side,
            'price': price,
            'quantity': quantity,
            'filled_quantity': quantity,  # Quote executions are always fills
            'status': 'FILLED',
            'parent_order_id': None,     # Main order, not a hedge
            'hedge_ratios': hedge_ratios,
            'hedge_order_ids': []
        }
        
        # Update position
        self.update_position(symbol, side, quantity)
        
        return order_id
        
    def add_hedge_order(self, order_id, symbol, side, price, quantity, parent_order_id):
        """Track a new hedge order"""
        self.open_orders[order_id] = {
            'symbol': symbol,
            'side': side,
            'price': price,
            'quantity': quantity,
            'filled_quantity': 0,
            'status': 'ACTIVE',
            'parent_order_id': parent_order_id,
            'hedge_ratios': None,      # Hedge orders don't have their own hedges
            'hedge_order_ids': []
        }
        
        # Link hedge order to parent
        parent_order = self.open_orders.get(parent_order_id)
        if parent_order:
            parent_order['hedge_order_ids'].append(order_id)
            
    def update_hedge_order(self, order_id, filled_quantity=None, new_status=None):
        """Update a hedge order's status and position"""
        order = self.open_orders.get(order_id)
        if not order:
            return
            
        if filled_quantity:
            new_fill_quantity = filled_quantity - order.get('filled_quantity', 0)
            if new_fill_quantity > 0:
                order['filled_quantity'] = filled_quantity
                self.update_position(order['symbol'], order['side'], new_fill_quantity)
            
        if new_status:
            order['status'] = new_status
            
        # If this is a hedge order that was rejected or cancelled
        if (new_status in ['REJECTED', 'CANCELLED'] and 
            order['parent_order_id']):
            parent_order = self.open_orders.get(order['parent_order_id'])
            if parent_order:
                # Remove this hedge order from parent's list
                parent_order['hedge_order_ids'].remove(order_id)
                print(f"Removed failed hedge {order_id} from parent {order['parent_order_id']}")
            
    def update_position(self, symbol, side, quantity):
        """Update position for an instrument"""
        side_multiplier = 1 if side == '1' else -1
        self.positions[symbol] = self.positions.get(symbol, 0) + (quantity * side_multiplier)

class MarketMaker:
    def __init__(self, client, order_manager):
        self.client = client
        self.order_manager = order_manager
        self.min_edge = 0.1  # 10% minimum edge per leg
        self.max_position = 10  # Maximum position size per instrument
        self.hedge_slippage = 0.0002  # Expected slippage for hedge execution (0.02%)
        
    def calculate_hedge_cost(self, symbol, quantity, side):
        """
        Calculate the total cost of hedging including slippage
        Returns (total_cost, hedge_ratios) or (None, None) if hedging not possible
        """
        expiry, strike, option_type = get_option_details(symbol)
        if not all([expiry, strike, option_type]):
            return None, None
            
        opposite_type = 'P' if option_type == 'C' else 'C'
        opposite_symbol = f"BTC-{expiry}-{strike}-{opposite_type}"
        future_symbol = f"BTC-{expiry}"
        
        # Get order books
        opp_book = self.client.order_book.get(opposite_symbol)
        future_book = self.client.order_book.get(future_symbol)
        
        if not all([opp_book, future_book]):
            return None, None
            
        # Calculate hedge prices including slippage
        if side == '1':  # If we're buying the option
            # We'll need to sell the opposite option and sell futures
            opp_price, opp_vol = calculate_weighted_price(
                opp_book['bid']['price'],
                opp_book['bid']['volume'],
                quantity
            )
            future_price, future_vol = calculate_weighted_price(
                future_book['bid']['price'],
                future_book['bid']['volume'],
                quantity
            )
        else:  # If we're selling the option
            # We'll need to buy the opposite option and buy futures
            opp_price, opp_vol = calculate_weighted_price(
                opp_book['ask']['price'],
                opp_book['ask']['volume'],
                quantity
            )
            future_price, future_vol = calculate_weighted_price(
                future_book['ask']['price'],
                future_book['ask']['volume'],
                quantity
            )
            
        if not all([opp_price, future_price, opp_vol >= quantity, future_vol >= quantity]):
            return None, None
            
        # Apply slippage to hedge prices
        if side == '1':
            opp_price *= (1 - self.hedge_slippage)
            future_price *= (1 - self.hedge_slippage)
        else:
            opp_price *= (1 + self.hedge_slippage)
            future_price *= (1 + self.hedge_slippage)
            
        # Calculate total hedge cost
        total_cost = (opp_price + future_price - float(strike)) if option_type == 'C' else \
                    (opp_price - future_price + float(strike))
                    
        hedge_ratios = {
            opposite_symbol: 1.0,    # 1:1 hedge with opposite option
            future_symbol: 1.0       # 1:1 hedge with future (simplified delta)
        }
        
        return total_cost, hedge_ratios
        
    def get_profitable_quotes(self, symbol):
        """
        Calculate bid/ask quotes that ensure profit after hedging
        Returns (bid_price, bid_size, ask_price, ask_size, bid_hedge_ratios, ask_hedge_ratios) 
        or None if can't quote
        """
        # Parse option details
        expiry, strike, option_type = get_option_details(symbol)
        if not all([expiry, strike, option_type]):
            return None
            
        base_quantity = 1.0  # Base quote size
        position = self.order_manager.positions.get(symbol, 0)
        existing_quote = self.order_manager.quotes.get(symbol, {})
        
        # Calculate sizes with bounds checking
        bid_size = max(0, min(
            base_quantity * (1 - position/self.max_position),
            self.max_order_size
        ))
        ask_size = max(0, min(
            base_quantity * (1 + position/self.max_position),
            self.max_order_size
        ))
        
        # Get theoretical prices and hedge lists
        theo_bid, bid_hedge_list = self.calculate_theoretical_price(symbol, "bid")
        theo_ask, ask_hedge_list = self.calculate_theoretical_price(symbol, "ask")
        
        # Handle cases where we can't calculate theoretical prices
        existing_bid_size = existing_quote.get("bid_size", 0)
        existing_ask_size = existing_quote.get("ask_size", 0)
        
        if not theo_bid and existing_bid_size != 0:
            bid_size = 0
        if not theo_ask and existing_ask_size != 0:
            ask_size = 0
            
        # If we can't calculate either theoretical price and have no existing quotes,
        # or if both sides would be zero size, return None
        if (not theo_bid and not theo_ask and not existing_quote) or (bid_size == 0 and ask_size == 0):
            return None
            
        # # Calculate hedge costs for both sides
        # bid_hedge_cost, bid_hedge_ratios = self.calculate_hedge_cost(symbol, bid_size, '1')
        # ask_hedge_cost, ask_hedge_ratios = self.calculate_hedge_cost(symbol, ask_size, '2')
        
        # if not all([bid_hedge_cost, ask_hedge_cost]):
        #     return None
            
        # # Calculate profitable prices
        # # For bid: Our price must be less than theoretical by more than hedge cost + min edge
        # # For ask: Our price must be more than theoretical by more than hedge cost + min edge
        # # Calculate profitable prices with hedge costs
        # bid_price = theo_bid - bid_hedge_cost - self.min_edge if theo_bid else None
        # ask_price = theo_ask + ask_hedge_cost + self.min_edge if theo_ask else None
        
        # # Ensure bid-ask spread is valid when both prices exist
        # if bid_price and ask_price and bid_price >= ask_price:
        #     return None
        
        # bid_data = [bid_price, bid_size, bid_hedge_ratios] if (bid_price or (existing_bid_size and bid_price is None)) else [None, None, None]
        # ask_data = [ask_price, ask_size, ask_hedge_ratios] if (ask_price or (existing_ask_size and ask_price is None)) else [None, None, None]
        
        # return (*bid_data[:2], *ask_data[:2], bid_data[2], ask_data[2])
        
        # Calculate hedge ratios (for backwards compatibility)
        bid_hedge_ratios = {}
        ask_hedge_ratios = {}
        
        if bid_hedge_list:
            for hedge in bid_hedge_list:
                bid_hedge_ratios[hedge[0]] = hedge[1]  # symbol: size
                
        if ask_hedge_list:
            for hedge in ask_hedge_list:
                ask_hedge_ratios[hedge[0]] = hedge[1]  # symbol: size
        
        # Calculate profitable prices with hedge costs
        bid_price = theo_bid - self.min_edge if theo_bid else None
        ask_price = theo_ask + self.min_edge if theo_ask else None
        
        # Ensure bid-ask spread is valid when both prices exist
        if bid_price and ask_price and bid_price >= ask_price:
            return None
        
        bid_data = [bid_price, bid_size, bid_hedge_ratios, bid_hedge_list] if (bid_price or (existing_bid_size and bid_price is None)) else [None, None, None, None]
        ask_data = [ask_price, ask_size, ask_hedge_ratios, ask_hedge_list] if (ask_price or (existing_ask_size and ask_price is None)) else [None, None, None, None]
        
        return (*bid_data[:2], *ask_data[:2], bid_data[2], ask_data[2], bid_data[3], ask_data[3])

            
        
    def send_mass_quotes(self, quote_list):
        """
        Send mass quotes for multiple instruments
        quote_list: list of (symbol, bid, bid_size, ask, ask_size, bid_hedge_ratios, ask_hedge_ratios,
                            bid_hedge_symbols, ask_hedge_symbols)
        """
        if not quote_list:
            return
            
        msg = self.client.create_message("i")  # Mass Quote
        quote_id = f"quote_{int(time.time()*1000)}" + hex(random.getrandbits(32))
        msg.append_pair(117, quote_id)  # QuoteID
        msg.append_pair(295, len(quote_list))  # NoQuoteEntries
        
        for quote_entry in quote_list:
            symbol, bid_price, bid_size, ask_price, ask_size, bid_hedges, ask_hedges, bid_hedge_symbols, ask_hedge_symbols = quote_entry
            
            msg.append_pair(55, symbol)  # Symbol
            
            if bid_price is not None:
                msg.append_pair(132, bid_price)  # BidPrice
                msg.append_pair(134, bid_size)   # BidSize
                
            if ask_price is not None:
                msg.append_pair(133, ask_price)  # OfferPrice
                msg.append_pair(135, ask_size)   # OfferSize
                
            # Track the quote with hedge symbols
            self.order_manager.add_quote(
                symbol, quote_id, bid_price, bid_size, ask_price, ask_size,
                bid_hedges, ask_hedges, bid_hedge_symbols, ask_hedge_symbols
            )
            
        self.client.send_message(msg)
        
    def update_market_making_quotes(self):
        """Update quotes across all instruments"""
        quote_updates = []
        
        for symbol in self.client.instruments:
            # Skip if it's not an option
            if symbol[-1] not in ['C', 'P']:
                continue
                
            quotes = self.get_profitable_quotes(symbol)
            if quotes:
                quote_updates.append(quotes)
                
        if quote_updates:
            self.send_mass_quotes(quote_updates)
    
    
    
    def calculate_fair_value(self, symbol, depth=1.0):
        """Calculate fair value for an option based on market data"""
        order_book = self.client.get_order_book(symbol)
        if not order_book:
            return None
            
        # Get weighted average prices for the specified depth
        bid_price, bid_vol = calculate_weighted_price(
            order_book['bid']['price'],
            order_book['bid']['volume'],
            depth
        )
        
        ask_price, ask_vol = calculate_weighted_price(
            order_book['ask']['price'],
            order_book['ask']['volume'],
            depth
        )
        
        if not all([bid_price, ask_price, bid_vol, ask_vol]):
            return None
            
        # Calculate mid price
        return (bid_price + ask_price) / 2
    
    
    def execute_hedge(self, symbol, original_side, quantity):
        """Execute hedging orders for option fills"""
        expiry, strike, option_type = get_option_details(symbol)
        if not all([expiry, strike, option_type]):
            return
            
        # Determine hedge instruments
        opposite_type = 'P' if option_type == 'C' else 'C'
        opposite_symbol = f"BTC-{expiry}-{strike}-{opposite_type}"
        future_symbol = f"BTC-{expiry}"
        
        # Calculate hedge ratios (simplified - should be based on proper Greeks)
        option_delta = 0.5  # Simplified delta calculation
        
        # Place hedge orders
        hedge_side = '1' if original_side == '2' else '2'  # Opposite of original trade
        
        # Hedge with opposite option
        self.client.send_new_order(
            symbol=opposite_symbol,
            side=hedge_side,
            quantity=quantity,
            price=self.calculate_fair_value(opposite_symbol)
        )
        
        # Hedge with futures
        self.client.send_new_order(
            symbol=future_symbol,
            side=hedge_side,
            quantity=quantity * option_delta,
            price=self.calculate_fair_value(future_symbol)
        )
            
    def handle_fill(self, order_id, filled_quantity, side):
        """Handle order fills and execute hedges"""
        if order_id not in self.order_manager.open_orders:
            return
            
        order = self.order_manager.open_orders[order_id]
        if side == "1":
            bidask = "bid"
        if side == "2":
            bidask = "ask"
        # Execute hedges based on stored ratios
        for hedge in order[bidask + '_hedge_symbols']:
            hedge_symbol = hedge[0]
            hedge_qty = hedge[1]
            hedge_side = hedge[2] # Opposite side
            
            if hedge_symbol[-1:] == "P" or hedge_symbol[-1:] == "C":
                hedge_qty = hedge[1]
            else: # if not a option we need to scale up the volume to match the future 
                hedge_qty = hedge[1] * self.client.get_order_book(hedge_symbol)['ask']['price'][0]
            
            # Get current market price for hedge
            order_book = self.client.get_order_book(hedge_symbol)
            if not order_book:
                continue
                
            # Use aggressive price to ensure execution
            if hedge_side == '1':  # Buy
                prices = order_book['ask']['price']
                volumes = order_book['ask']['volume']
            else:  # Sell
                prices = order_book['bid']['price']
                volumes = order_book['bid']['volume']
                
            hedge_price, _ = calculate_weighted_price(prices, volumes, hedge_qty)
            if not hedge_price:
                continue
                
            # Place hedge order
            hedge_order_id = self.client.send_new_order(
                hedge_symbol,
                hedge_price,
                hedge_qty,
                hedge_side
            )
            
            if hedge_order_id:
                order['hedge_orders'].append(hedge_order_id)
                
        # Update position after hedging
        self.order_manager.update_order(order_id, 'FILLED', filled_quantity)

class DeribitFIXClient:
    def __init__(self, host, port, sender_comp_id, target_comp_id, username, password):
        self.host = host
        self.port = port
        self.sender_comp_id = sender_comp_id
        self.target_comp_id = target_comp_id
        self.username = username
        self.password = password
        self.seq_num = 1
        self.socket = None
        self.parser = simplefix.FixParser()
        self.running = False
        
        # Attributes for storing market data
        self.current_index = None
        self.order_book = {}
        self.instruments = set()  # Store discovered instruments
        self.security_list_received = False
        
        self.open_orders = {}  # Dictionary to track our open orders
        self.theoretical_prices = {}  # Store calculated theoretical prices

    def connect(self):
        """Establish connection and start message handling"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
        self.running = True
        
        # Start message handling thread
        Thread(target=self.message_handler, daemon=True).start()
        
        # Send logon message
        self.send_logon()
        
    def setup_market_maker(self):
        """Initialize market making components"""
        self.order_manager = OrderManager()
        self.market_maker = MarketMaker(self, self.order_manager)
        
        # Start market making update thread
        Thread(target=self.market_making_loop, daemon=True).start()
        
    def market_making_loop(self):
        """Main market making loop"""
        while self.running:
            try:
                if self.security_list_received:  # Only start once we have instruments
                    self.market_maker.update_market_making_quotes()
                time.sleep(0.1)  # Run frequently to keep quotes updated
            except Exception as e:
                print(f"Market making error: {e}")

    def handle_message(self, msg):
        """Process different message types"""
        msg_type = msg.get(35)  # MsgType tag
        
        if msg_type == b"W":  # Market Data Snapshot
            self.handle_market_snapshot(msg)
        elif msg_type == b"Y":  # Incremental market updates
            print("ææææææææææ")
            print(msg)
        elif msg_type == b"X":  # Incremental market update
            self.handle_incremental_market_data(msg)
        elif msg_type == b"0":  # Heartbeat
            self.heartbeat()
        elif msg_type == b"8":  # Execution Report
            self.handle_execution_report(msg)
        elif msg_type == b"A":  # Logon response
            print("Logon successful")
            # Request security list first
            self.request_security_list()
        elif msg_type == b"y":  # Security List
            self.handle_security_list(msg)

    def handle_security_list(self, msg):
        """Handle Security List response"""
        print(msg)
        for field in msg.pairs:
            tag, value = field
            tag = tag.decode('utf-8')
            if tag == "55":  # Symbol tag
                symbol = value.decode('utf-8')
                # Check if the symbol matches our desired expiry dates
                for expiry in option_list:
                    if expiry in symbol:
                        self.instruments.add(symbol)
                        print(f"Added instrument: {symbol}")

        # Check if this is the last message in the security list
        if msg.get(10):  # LastFragment
            self.security_list_received = True
            print(f"Security list complete. Found {len(self.instruments)} matching instruments")
            # Now that we have the complete list, subscribe to market data
            self.subscribe_market_data()

    def subscribe_market_data(self):
        """Subscribe to market data for all discovered instruments"""
        msg = self.create_message("V")  # MarketDataRequest
        
        msg.append_pair(262, "1")  # MDReqID
        msg.append_pair(263, "1")  # SubscriptionRequestType (1 = Subscribe)
        msg.append_pair(264, "0")  # MarketDepth
        msg.append_pair(265, "1")  # Update type (1=Incremental refresh)
        
        # Entry types (Bid, Ask, Trade)
        msg.append_pair(267, 3)  # NoMDEntryTypes
        msg.append_pair(269, "0")  # MDEntryType - Bid
        msg.append_pair(269, "1")  # MDEntryType - Ask
        msg.append_pair(269, "2")  # MDEntryType - Trade
        
        # Add base instruments (index and perpetual)
        all_instruments = list(self.instruments)
        all_instruments.extend(["BTC_USDT-DERIBIT-INDEX", instrument_name])
        
        # Add all symbols
        msg.append_pair(146, len(all_instruments))  # NoRelatedSym
        for instrument in all_instruments:
            msg.append_pair(55, instrument)  # Symbol
        
        self.send_message(msg)
        print(f"Subscribed to {len(all_instruments)} instruments")

    def request_security_list(self):
        """Request list of all instruments"""
        msg = self.create_message("x")  # Security List Request
        
        msg.append_pair(320, "req1")  # SecurityReqID
        msg.append_pair(559, "4")     # SecurityListRequestType (0 = Symbol)
        msg.append_pair(263, "0")     # SubscriptionRequestType 0 = Snapshot, 1 = Snapshot + Subscribe, 2 = Unsubscribe

        
        self.send_message(msg)
          
        
    def disconnect(self):
        """Clean disconnect from server"""
        self.running = False
        if self.socket:
            self.socket.close()
            
    def message_handler(self):
        """Handle incoming FIX messages"""
        buffer = b""
        while self.running:
            try:
                data = self.socket.recv(4096)
                if not data:
                    print("Connection closed by server")
                    break
                    
                buffer += data
                while b"\x01" in buffer:
                    idx = buffer.find(b"\x01")
                    msg_data = buffer[:idx+1]
                    buffer = buffer[idx+1:]
                    
                    self.parser.append_buffer(msg_data)
                    while True:
                        msg = self.parser.get_message()
                        if msg is None:
                            break
                        self.handle_message(msg)
                    
                        
            except socket.error as e:
                print(f"Socket error: {e}")
                break
                

    def handle_market_snapshot(self, msg):
        """Process market data messages"""
        symbol = msg.get(55)  # Symbol tag
        if symbol:
            symbol = symbol.decode('utf-8')
            #print(f"\nMarket Data for {symbol}")
        
        # Initialize order book for the symbol if not exists
        if symbol and symbol not in self.order_book:
            self.order_book[symbol] = {
                'bid': {'price': [], 'volume': []},
                'ask': {'price': [], 'volume': []}
            }
        
        # New parsing method for market data
        md_entries = []
        current_entry = {}
        
        # Iterate through all message fields
        for field in msg.pairs:
            tag, value = field
            tag = int(tag)
            value = value.decode('utf-8')
            
            # MDEntry related tags
            if tag == 269:  # MDEntryType
                if current_entry:
                    md_entries.append(current_entry)
                current_entry = {'type': value}
            elif tag == 270:  # MDEntryPx
                current_entry['price'] = float(value)
            elif tag == 271:  # MDEntrySize
                current_entry['size'] = float(value)
        
        # Add last entry
        if current_entry:
            md_entries.append(current_entry)
        
        # Process market data entries
        for entry in md_entries:
            type_desc = {
                '0': 'Bid',
                '1': 'Ask',
                '2': 'Trade'
            }.get(entry.get('type'), 'Unknown')
            
            #print(f"{type_desc}: Price={entry.get('price', 'N/A')} Size={entry.get('size', 'N/A')}")
            
            # Handle different entry types
            if type_desc == 'Bid' and symbol:
                # Update bid side of the order book
                self.order_book[symbol]['bid']['price'].append(entry.get('price', 0))
                self.order_book[symbol]['bid']['volume'].append(entry.get('size', 0))
                # Sort bids in descending order (highest bid first)
                bid_prices = self.order_book[symbol]['bid']['price']
                bid_volumes = self.order_book[symbol]['bid']['volume']
                sorted_indices = sorted(range(len(bid_prices)), key=lambda k: bid_prices[k], reverse=True)
                self.order_book[symbol]['bid']['price'] = [bid_prices[i] for i in sorted_indices]
                self.order_book[symbol]['bid']['volume'] = [bid_volumes[i] for i in sorted_indices]
            
            elif type_desc == 'Ask' and symbol:
                # Update ask side of the order book
                self.order_book[symbol]['ask']['price'].append(entry.get('price', 0))
                self.order_book[symbol]['ask']['volume'].append(entry.get('size', 0))
                # Sort asks in ascending order (lowest ask first)
                ask_prices = self.order_book[symbol]['ask']['price']
                ask_volumes = self.order_book[symbol]['ask']['volume']
                sorted_indices = sorted(range(len(ask_prices)), key=lambda k: ask_prices[k])
                self.order_book[symbol]['ask']['price'] = [ask_prices[i] for i in sorted_indices]
                self.order_book[symbol]['ask']['volume'] = [ask_volumes[i] for i in sorted_indices]
            
            elif type_desc == 'Unknown':
                # Assume this is an index update
                if symbol:
                    self.current_index = entry.get('price', 0)
                    print(f"Updated Index for {symbol}: {self.current_index}")
                    
    def handle_incremental_market_data(self, msg):
        """
        Handle incremental market data updates (message type 'X')
        
        This method processes incremental updates for the order book, supporting:
        - 0 (New): Add new order to the order book
        - 1 (Change): Update existing order's volume
        - 2 (Delete): Remove an order from the order book
        """
        symbol = None
        md_entries = []
        current_entry = {}
        top_of_book_changed = False # Bool to check for change in best bid/ask
        
        # Parse message to extract relevant information
        for field in msg.pairs:
            tag, value = field
            tag = int(tag)
            value = value.decode('utf-8')
            
            if tag == 55:  # Symbol
                symbol = value
            
            # MDEntry related tags
            if tag == 279:  # MDUpdateAction
                if current_entry:
                    md_entries.append(current_entry)
                current_entry = {'update_action': value}
            elif tag == 270:  # MDEntryPx
                current_entry['price'] = float(value)
            elif tag == 271:  # MDEntrySize
                current_entry['size'] = float(value)
            elif tag == 269:  # MDEntryType
                current_entry['type'] = value
                

        # Add last entry
        if current_entry:
            md_entries.append(current_entry)
        
        # Initialize order book for symbol if not exists
        if symbol and symbol not in self.order_book:
            self.order_book[symbol] = {
                'bid': {'price': [], 'volume': []},
                'ask': {'price': [], 'volume': []}
            }            
            
        # Process each market data entry
        for entry in md_entries:
            type_desc = {
                '0': 'Bid',
                '1': 'Ask',
                '2': 'Trade',
                '3': 'Index'
            }.get(entry.get('type'), 'Unknown')
            
            update_action = {
                '0': 'New',
                '1': 'Change', 
                '2': 'Delete'
            }.get(entry.get('update_action'), 'Unknown')
            
            #print(f"Incremental Update: {type_desc} {update_action}")
            
            # Determine which side of the order book to update
            book_side = None
            if type_desc == 'Bid':
                book_side = self.order_book[symbol]['bid']
            elif type_desc == 'Ask':
                book_side = self.order_book[symbol]['ask']
            elif type_desc == 'Index':
                # Updates the current index with a new price
                self.current_index = entry.get('price')
                # Check the trade logic everytime there is an update to the index
                self.trade_logic()
                print("indexPrice: ", self.current_index)
            
            if not book_side:
                continue
            
            # Handle different update actions
            if update_action == 'New':
                # Find correct insertion point to maintain sorted order
                insertion_index = 0
                if type_desc == 'Bid':
                    # For bids, insert in descending order
                    while (insertion_index < len(book_side['price']) and 
                           entry['price'] < book_side['price'][insertion_index]):
                        insertion_index += 1
                    if insertion_index == 0:  # New best bid
                        top_of_book_changed = True
                else:
                    # For asks, insert in ascending order
                    while (insertion_index < len(book_side['price']) and 
                           entry['price'] > book_side['price'][insertion_index]):
                        insertion_index += 1
                    if insertion_index == 0:  # New best ask
                        top_of_book_changed = True
                
                # Insert price and volume at the correct position
                book_side['price'].insert(insertion_index, entry['price'])
                book_side['volume'].insert(insertion_index, entry['size'])
            
            elif update_action == 'Change':
                # Find the price to update
                try:
                    update_index = book_side['price'].index(entry['price'])
                    book_side['volume'][update_index] = entry['size']
                    if update_index == 0:  # Volume change at top of book
                        top_of_book_changed = True
                except ValueError:
                    print(f"Warning: Could not find price {entry['price']} to update")
            
            elif update_action == 'Delete':
                # Find and remove the price and its corresponding volume
                try:
                    delete_index = book_side['price'].index(entry['price'])
                    if delete_index == 0:  # Deleting top of book
                        top_of_book_changed = True
                    del book_side['price'][delete_index]
                    del book_side['volume'][delete_index]
                except ValueError:
                    print(f"Warning: Could not find price {entry['price']} to delete")
        
            # Optional: Print updated order book for debugging
            #if len(self.order_book[symbol]['bid']['price']) > 1:
            #    print(f"Updated Order Book for {symbol} with {update_action} at {entry['price']} with size {entry['size']}")
        #if symbol == "BTC_USDT-DERIBIT-INDEX":
        #    print(f"Updated Order Book for {symbol} ")
        #print(f"Bids: {list(zip(self.order_book[symbol]['bid']['price'], self.order_book[symbol]['bid']['volume']))}")
        #print(f"Asks: {list(zip(self.order_book[symbol]['ask']['price'], self.order_book[symbol]['ask']['volume']))}")
        # After processing the entries, check if this was an option update
        
        
        
        # if symbol[-1] == "C" or symbol[-1] == "P":
        #     if top_of_book_changed:
        #         theoretical_price = self.calculate_theoretical_price(symbol, type_desc.lower())
        #         #print("THEO",theoretical_price)
        #         if theoretical_price is not None:
        #             if self.theoretical_prices.get(symbol) is not None:
        #                 old_price = self.theoretical_prices.get(symbol)
        #                 # Log if price changed significantly
        #                 if old_price is None or abs(theoretical_price - old_price) > 0.0001:
        #                     print(f"New theoretical price for {symbol}: {theoretical_price:.4f}")
        #             self.theoretical_prices[symbol] = theoretical_price
                    
                    
                    

                            
    def calculate_theoretical_price(self, symbol, bidask):
        """
        We receive a new order to the order book which we assume replaces the 
        best bid or best ask for that symbol
            - We are interested in calculating the price which according to 
                PCP is fair. 
            - We then want to use this price + a margin to price the opposite 
                option in the order book. This is how we make money
        Example;
            - We receive a new best bid for a call (someone wants to buy)
            - This will be our hedge when we get a fill on our put bid
                (we are trying to buy)
            - The price calculated in this function is the fair value 
                of the put bid we have in the market
            - When we receive a new bid, we want to calculate a bid price for 
                the opposite option as well
                
        Calculate theoretical price based on put-call parity
        C - P = F - K * e^(-r*t)
        
        We'll simplify by assuming r ≈ 0 for short-term options, so:
        C - P = F - K
        Therefore:
        C = P + F - K  (for calls)
        P = C - F + K  (for puts)
        """
        expiry, strike, option_type = get_option_details(symbol)
        if not all([expiry, strike, option_type]):
            return None
            
        # Find the corresponding opposite option
        opposite_type = 'P' if option_type == 'C' else 'C'
        opposite_symbol = f"BTC-{expiry}-{strike}-{opposite_type}"
        future_symbol = f"BTC-{expiry}"
        
        # Check if we have order books for both options and future
        if not all(sym in self.order_book for sym in [symbol, opposite_symbol, future_symbol]):
            return None
            
        # Get best prices considering depth for 1.0 contract
        target_volume = 1.0
        
        # For opposite option
        opp_book = self.order_book[opposite_symbol]
        opp_price, opp_vol = calculate_weighted_price(
            opp_book[bidask]['price'],
            opp_book[bidask]['volume'],
            target_volume
        )
            
        if not opp_price or opp_vol < target_volume:
            return None
            
        # For future
        future_book = self.order_book[future_symbol]
        if (bidask == "bid" and option_type == "C") or (bidask == "ask" and option_type == "P"):
            future_price, future_vol = calculate_weighted_price(
                future_book['ask']['price'],
                future_book['ask']['volume'],
                target_volume
            )       
            future_side = "buy"
        else:
            future_price, future_vol = calculate_weighted_price(
                future_book['bid']['price'],
                future_book['bid']['volume'],
                target_volume
            )
            future_side = "sell"

        if not future_price or future_vol < target_volume:
            return None
            
        # Calculate theoretical price
        if option_type == 'C':
            theoretical_price = (opp_price*future_price + future_price - int(strike))/future_price
        else:
            theoretical_price = (opp_price*future_price - future_price + int(strike))/future_price
            
        # Determine hedge directions
        opp_side = "1" if bidask == "ask" else "2"  # Opposite direction of main quote
        
        # Create hedge list with [symbol, size, direction]
        hedge_list = [
            [opposite_symbol, target_volume, opp_side],  # Hedge with opposite option
            [future_symbol, target_volume, future_side]  # Hedge with future
        ]
        
        return theoretical_price, hedge_list
    
    
    def handle_execution_report(self, msg):
        """
        Handle execution reports using OrderStatus (39)
        0 = New
        1 = Partially filled
        2 = Filled
        4 = Cancelled
        8 = Rejected
        """
        order_status = msg.get(39)
        if not order_status:
            return
        order_status = order_status.decode('utf-8')
        
        # Extract common fields
        symbol = msg.get(55).decode('utf-8') if msg.get(55) else None
        side = msg.get(54).decode('utf-8') if msg.get(54) else None # 1 = Buy, 2 = Sell
        price = float(msg.get(44).decode('utf-8')) if msg.get(44) else None
        quantity = float(msg.get(32).decode('utf-8')) if msg.get(32) else None  # LastQty
        order_id = msg.get(11).decode('utf-8') if msg.get(11) else None        # ClOrdID
        
        # Mass quote execution creates a new order
        is_quote_execution = msg.get(299)  # Quote entry ID, only for mass quote
        
        if is_quote_execution:
            if not all([symbol, side, price, quantity]):
                print(f"Missing required fields in quote execution: {msg}")
                return
                
            # Create tracked order from quote execution
            main_order_id = self.order_manager.add_fill_from_quote(
                symbol, side, price, quantity)
                
            if main_order_id:
                # Initiate hedging
                if side:
                    self.market_maker.handle_fill(main_order_id, quantity, side)
                else:
                    print("Warning!! No side detected unable to initiate hedge!")
                
        # Handle regular order updates (including hedge orders)
        elif order_id:
            if order_status == '0':  # New
                print(f"Order {order_id} accepted")
                
            elif order_status == '1':  # Partially filled
                if quantity:
                    print(f"Order {order_id} partially filled: {quantity} @ {price}")
                    self.order_manager.update_hedge_order(
                        order_id,
                        filled_quantity=quantity,
                        new_status='PARTIALLY_FILLED'
                    )
                    
            elif order_status == '2':  # Filled
                if quantity:
                    print(f"Order {order_id} filled: {quantity} @ {price}")
                    self.order_manager.update_hedge_order(
                        order_id,
                        filled_quantity=quantity,
                        new_status='FILLED'
                    )
                    
            elif order_status == '4':  # Cancelled
                print(f"Order {order_id} cancelled")
                self.order_manager.update_hedge_order(
                    order_id,
                    new_status='CANCELLED'
                )
                
            elif order_status == '8':  # Rejected
                print(f"Order {order_id} rejected")
                self.order_manager.update_hedge_order(
                    order_id,
                    new_status='REJECTED'
                )
                
                # Handle hedge rejection - might need to try alternative hedge
                order = self.order_manager.open_orders.get(order_id)
                if order and order['parent_order_id']:
                    print(f"Hedge order rejected for main order {order['parent_order_id']}")
                    # Could add retry logic here
                
    def send_new_order(self, price, quantity, side='2'):  # '2' for Sell
        """Send a new limit order"""
        msg = self.create_message("D")  # New Order Single
        
        # Generate a unique ClOrdID
        cl_ord_id = f"order_{int(time.time()*1000)}" + hex(random.getrandbits(32))
        
        msg.append_pair(11, cl_ord_id)  # ClOrdID
        msg.append_pair(55, instrument_name)  # Symbol
        msg.append_pair(54, side)  # Side (2=Sell)
        msg.append_pair(40, "2")  # OrderType (2=Limit)
        msg.append_pair(44, price)  # Price
        msg.append_pair(38, quantity)  # OrderQty
        msg.append_pair(59, "1")  # TimeInForce (1=Good Till Cancel)
        msg.append_pair(18, "6")  # 6 = Post only
        print(f"Posting new order {cl_ord_id} at {price}")
        self.send_message(msg)
        return cl_ord_id
        
    def modify_order(self, order_id, new_price, new_quantity=None):
        """Modify an existing order"""
        if order_id not in self.open_orders:
            print(f"Order {order_id} not found in open orders")
            return
            
        msg = self.create_message("G")  # Order Cancel/Replace Request
        
        # Generate a unique ClOrdID for the modification
        cl_ord_id = f"modify_{int(time.time()*1000)}" + hex(random.getrandbits(32))
        
        msg.append_pair(11, order_id)  #  OrigClOrdID
        #msg.append_pair(41, cl_ord_id)  #  ClOrdID
        msg.append_pair(55, instrument_name)  # Symbol
        msg.append_pair(54, "2")  # Side (2=Sell)
        msg.append_pair(40, "2")  # OrderType (2=Limit)
        msg.append_pair(44, new_price)  # Price
        #msg.append_pair(38, 2)  # 6 = Post only
        msg.append_pair(18, "6")  # 6 = Post only
        
        #if new_quantity:
        #    msg.append_pair(38, new_quantity)  # OrderQty
        #else:
        #    msg.append_pair(38, self.open_orders[order_id]['remaining_quantity'])
        print(f"Modifying order {order_id} to {new_price}")
        self.send_message(msg)
        
    def cancel_order(self, order_id):
        """Cancel an existing order"""
        if order_id not in self.open_orders:
            print(f"Order {order_id} not found in open orders")
            return
            
        msg = self.create_message("F")  # Order Cancel Request
        
        # Generate a unique ClOrdID for the cancellation
        cl_ord_id = f"cancel_{int(time.time()*1000)}" + hex(random.getrandbits(32))
        
        msg.append_pair(11, order_id)  #  OrigClOrdID
        #msg.append_pair(41, cl_ord_id)  #  ClOrdID
        msg.append_pair(55, instrument_name)  # Symbol
        msg.append_pair(54, "1")  # Side (1=Sell)
        print(f"Cancelling order {order_id}")
        self.send_message(msg)
        
    def trade_logic(self):
        """
        Implement trading logic for ask order management.
        Places or updates ask order based on index price and best bid.
        """


            
    # Getter methods to access current index and order book
    def get_current_index(self):
            """Return the current index value"""
            return self.current_index
    
    def get_order_book(self, symbol=None):
        """
        Return the entire order book or specific symbol's order book
        
        :param symbol: Optional. Specific symbol to retrieve order book for
        :return: Order book dictionary
        """
        if symbol:
            return self.order_book.get(symbol, {})
        return self.order_book

                        
    def create_message(self, msg_type):
        """Create a new FIX message with common header fields"""
        msg = simplefix.FixMessage()
        msg.append_pair(8, "FIX.4.4")  # BeginString
        msg.append_pair(35, msg_type)   # MsgType
        msg.append_pair(49, self.sender_comp_id)  # SenderCompID
        msg.append_pair(56, self.target_comp_id)  # TargetCompID
        msg.append_pair(52, datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3])  # SendingTime
        msg.append_pair(34, self.seq_num)  # MsgSeqNum
        self.seq_num += 1
        return msg
        
    def send_logon(self):
        """Send Logon message"""
        msg = self.create_message("A")  # Logon
        
        nonce = secrets.token_urlsafe()
        encodedBytes = base64.b64encode(nonce.encode("utf-8"))
        encodedNonce = str(encodedBytes, "utf-8")
        timestamp = time.time_ns()
        raw_data = str(timestamp)[:-6] + "." + encodedNonce
        base_signature = raw_data + self.password
        sha256 = hashlib.sha256(base_signature.encode('utf-8'))
        secret = sha256.hexdigest()
        secret = bytes.fromhex(secret)
        encodedBytes = base64.b64encode(secret)
        secret = str(encodedBytes, "utf-8")

        msg.append_pair(96, raw_data)   
        msg.append_pair(108, 10)  # HeartBtInt
        msg.append_pair(553, self.username)  # Username
        msg.append_pair(554, secret)  # Password
        msg.append_pair(9001, "Y")  # cancel on disconnect
        self.send_message(msg)
        

        
    def heartbeat(self):
        msg = self.create_message("0")  # MarketDataRequest
        self.send_message(msg)
        #print(self.order_book)
        

        
    def send_message(self, msg):
        print(msg)
        """Send a FIX message to the server"""
        msg_bytes = msg.encode()
        self.socket.send(msg_bytes)
client = 0
def main():
    global client
    # Configuration
    client = DeribitFIXClient(
        host="www.deribit.com",
        port=9881,  # Verify correct port with Deribit
        sender_comp_id="LSCM",
        target_comp_id="DERIBITSERVER",
        username="WLIKqVUD",
        password="iU4EJRV_UJgZ_VCULjmAZRIe91cmsS2CiEh6ns2Xez0"
    )
    
    try:
        client.connect()
        
        # Keep the main thread running
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nDisconnecting...")
        client.disconnect()
    except Exception as e:
        print(f"Error: {e}")
        client.disconnect()

if __name__ == "__main__":
    main()