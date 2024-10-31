import simplefix
import base64
import hashlib
import secrets
import socket
import time
import datetime
from threading import Thread

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
        
        # New attributes for storing market data
        self.current_index = None
        self.order_book = {}
        
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
                        
            except socket.error as e:
                print(f"Socket error: {e}")
                break
                
    def handle_message(self, msg):
        """Process different message types"""
        msg_type = msg.get(35)  # MsgType tag
        
        if msg_type == b"W":  # Market Data Snapshot
            self.handle_market_data(msg)
        elif msg_type == b"0":  # Heartbeat
            self.heartbeat()
        elif msg_type == b"A":  # Logon response
            print("Logon successful")
            # Subscribe to market data after successful logon
            self.subscribe_market_data()
            
    def handle_market_data(self, msg):
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
            
            print(f"{type_desc}: Price={entry.get('price', 'N/A')} Size={entry.get('size', 'N/A')}")
            
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
        """Subscribe to BTC-PERPETUAL and BTC Index market data"""
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
        msg.append_pair(55, "BTC-DERIBIT-INDEX")  # Symbol
        msg.append_pair(55, "BTC-PERPETUAL")      # Symbol
        
        self.send_message(msg)
        
    def heartbeat(self):
        msg = self.create_message("0")  # MarketDataRequest
        self.send_message(msg)
        print(self.order_book)
        
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

def main():
    # Configuration
    client = DeribitFIXClient(
        host="www.deribit.com",
        port=9881,  # Verify correct port with Deribit
        sender_comp_id="LSCM",
        target_comp_id="DERIBITSERVER",
        username="BiI1BTLc",
        password="Lll48doAqigKOhJNSuFMY9Dhwx8E6FoXqgNYVM5-nQ0"
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