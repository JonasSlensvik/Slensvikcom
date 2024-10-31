import simplefix
import base64
import hashlib
import secrets
import socket
import time
import datetime
from threading import Thread

instrument_name = "BTC_USDT-PERPETUAL"

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
        
        self.open_orders = {}  # Dictionary to track our open orders
        self.ALPHA = 0  # Constant alpha value for price calculation
        self.current_ask_order = None  # Track our current ask order
        
    def connect(self):
        """Establish connection and start message handling"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
        self.running = True
        
        # Start message handling thread
        Thread(target=self.message_handler, daemon=True).start()
        
        # Send logon message
        self.send_logon()
        
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
                        self.check_bid_change()
                        
            except socket.error as e:
                print(f"Socket error: {e}")
                break
                
    def handle_message(self, msg):
        """Process different message types"""
        msg_type = msg.get(35)  # MsgType tag
        
        if msg_type == b"W":  # Market Data Snapshot
            self.handle_market_snapshot(msg)
        elif msg_type == b"X":  # Incremental market updates
            self.handle_incremental_market_data(msg)
        elif msg_type == b"0":  # Heartbeat
            self.heartbeat()
        if msg_type == b"8":  # Execution Report
            self.handle_execution_report(msg)
        elif msg_type == b"A":  # Logon response
            print("Logon successful")
            # Subscribe to market data after successful logon
            self.subscribe_market_data()
            
    def handle_market_snapshot(self, msg):
        """Process market data messages"""
        symbol = msg.get(55)  # Symbol tag
        if symbol:
            symbol = symbol.decode('utf-8')
            print(f"\nMarket Data for {symbol}")
        
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
                else:
                    # For asks, insert in ascending order
                    while (insertion_index < len(book_side['price']) and 
                           entry['price'] > book_side['price'][insertion_index]):
                        insertion_index += 1
                
                # Insert price and volume at the correct position
                book_side['price'].insert(insertion_index, entry['price'])
                book_side['volume'].insert(insertion_index, entry['size'])
            
            elif update_action == 'Change':
                # Find the price to update
                try:
                    update_index = book_side['price'].index(entry['price'])
                    book_side['volume'][update_index] = entry['size']
                except ValueError:
                    print(f"Warning: Could not find price {entry['price']} to update")
            
            elif update_action == 'Delete':
                # Find and remove the price and its corresponding volume
                try:
                    delete_index = book_side['price'].index(entry['price'])
                    del book_side['price'][delete_index]
                    del book_side['volume'][delete_index]
                except ValueError:
                    print(f"Warning: Could not find price {entry['price']} to delete")
        
            # Optional: Print updated order book for debugging
        if len(self.order_book[symbol]['bid']['price']) > 1:
            print(f"Updated Order Book for {symbol} with {update_action} best bid is {self.order_book[symbol]['bid']['price'][0]}")
        #if symbol == "BTC_USDT-DERIBIT-INDEX":
        #    print(f"Updated Order Book for {symbol} ")
        #print(f"Bids: {list(zip(self.order_book[symbol]['bid']['price'], self.order_book[symbol]['bid']['volume']))}")
        #print(f"Asks: {list(zip(self.order_book[symbol]['ask']['price'], self.order_book[symbol]['ask']['volume']))}")
    
    # Updates the agressive ask order
    def check_bid_change(self, symbol=instrument_name):
        """
        Check if the best bid has changed and trigger trade execution.
        
        :param symbol: Symbol to check for bid changes (default "self.instrument_name")
        :return: Boolean indicating if best bid changed
        """

        # Ensure the symbol exists in the order book
        if symbol not in self.order_book or not self.order_book[symbol]['bid']['price']:
            return False
        # Get the current best bid (highest price)
        current_best_bid = self.order_book[symbol]['bid']['price'][0]
        
        # Check if this is different from the previously stored best bid
        if not hasattr(self, '_last_best_bid'):
            self._last_best_bid = current_best_bid
            return True
        #print(self._last_best_bid)
        #print(current_best_bid)

        # Compare current best bid with last recorded best bid
        if current_best_bid != self._last_best_bid:

            # Update last best bid
            old_bid = self._last_best_bid
            self._last_best_bid = current_best_bid
            
            # Optional: Print change details
            #print(f"Best bid changed from {old_bid} to {current_best_bid}")
            
            # Trigger trade execution function
            self.trade_logic()
            
            return True
        
        return False
    
    def handle_execution_report(self, msg):
        """Handle execution reports for order updates"""
        print(msg)
        order_id = msg.get(41)  # OrderID
        if order_id:
            order_id = order_id.decode('utf-8')
            
        exec_type = msg.get(150)  # ExecType
        if exec_type:
            exec_type = exec_type.decode('utf-8')
            
        order_status = msg.get(39)  # OrderStatus
        if order_status:
            order_status = order_status.decode('utf-8')
            
        # Update order tracking based on execution report
        if order_id and order_status and exec_type:
            if order_status == '0':  # New
                self.open_orders[order_id] = {
                    'status': order_status,
                    'price': float(msg.get(44).decode('utf-8')) if msg.get(44) else None,
                    'remaining_quantity': float(msg.get(151).decode('utf-8')) if msg.get(151) else None
                }
                print(self.open_orders)
            elif exec_type == '4':  # Cancelled
                if order_id in self.open_orders:
                    del self.open_orders[order_id]
                    self.current_ask_order = None
            elif exec_type == '5':  # Replace
                self.open_orders[order_id]['price'] = float(msg.get(44).decode('utf-8')) if msg.get(44) else None
                self.open_orders[order_id]['remaining_quantity'] = float(msg.get(151).decode('utf-8')) if msg.get(151) else None
                
    def send_new_order(self, price, quantity, side='2'):  # '2' for Sell
        """Send a new limit order"""
        msg = self.create_message("D")  # New Order Single
        
        # Generate a unique ClOrdID
        cl_ord_id = f"order_{int(time.time()*1000)}"
        
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
        cl_ord_id = f"modify_{int(time.time()*1000)}"
        
        msg.append_pair(11, order_id)  #  OrigClOrdID
        #msg.append_pair(41, cl_ord_id)  #  ClOrdID
        msg.append_pair(55, instrument_name)  # Symbol
        msg.append_pair(54, "2")  # Side (2=Sell)
        msg.append_pair(40, "2")  # OrderType (2=Limit)
        msg.append_pair(44, new_price)  # Price
        msg.append_pair(38, 2)  # 6 = Post only
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
        cl_ord_id = f"cancel_{int(time.time()*1000)}"
        
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
        if not self.current_index or instrument_name not in self.order_book:
            return
            
        # Calculate target price as max of (index - alpha) and (best bid + 1)
        index_based_price = self.current_index - self.ALPHA
        
        # Get best bid if available
        best_bid = None
        if (self.order_book[instrument_name]['bid']['price'] and 
            len(self.order_book[instrument_name]['bid']['price']) > 0):
            best_bid = self.order_book[instrument_name]['bid']['price'][0]
            
        bid_based_price = best_bid + 1 if best_bid is not None else float('inf')
        
        # Take the maximum of the two prices
        target_price = max(index_based_price, bid_based_price)
        print("Targetprice", target_price)
        # Check if we need to place or modify order
        if not self.current_ask_order:
            # Place new order
            order_id = self.send_new_order(price=target_price, quantity=20, side='2')
            self.current_ask_order = order_id
        else:
            # Check if price needs updating
            current_order = self.open_orders.get(self.current_ask_order)
            if current_order and abs(current_order['price'] - target_price) > 0.5:  # 0.5 as minimum price difference
                print("Should we update", abs(current_order['price'] - target_price))
                self.cancel_order(self.current_ask_order)
                #self.modify_order(self.current_ask_order, target_price)

            
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
        
    def subscribe_market_data(self):
        """Subscribe to instrument_name and BTC Index market data"""
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
        
        # Symbols
        msg.append_pair(146, 2)  # NoRelatedSym
        msg.append_pair(55, "BTC_USDT-DERIBIT-INDEX")  # Symbol
        msg.append_pair(55, instrument_name)      # Symbol
        
        self.send_message(msg)
        
    def heartbeat(self):
        msg = self.create_message("0")  # MarketDataRequest
        self.send_message(msg)
        #print(self.order_book)
        
    def request_security_list(self):
        msg = self.create_message("x")  # MarketDataRequest
        
        msg.append_pair(320, "intro")  # user generated id
        msg.append_pair(559, 0)  # 4 = ALL
        msg.append_pair(167, "INDEX")  # type
        
        self.send_message(msg)
        
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