import json
import time
import websocket
import threading
import hmac
import hashlib
import uuid
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# API credentials (empty as we're accessing public data)
api_key = ""
api_secret = ""

# WebSocket URL for Bybit public endpoints
ws_url = 'wss://stream.bybit.com/v5/public/spot'

# Topic for BTCUSDT order book - using 1 for testing (can be 1, 50, 200, 500)
topic = "orderbook.1.BTCUSDT"

class BybitWebSocket:
    def __init__(self, url, topics):
        self.url = url
        self.topics = topics if isinstance(topics, list) else [topics]
        self.ws = None
        self.connected = False
        self.reconnect_count = 0
        self.max_reconnects = 5
        self.reconnect_interval = 5  # seconds

    def connect(self):
        """Establish WebSocket connection"""
        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp(
            self.url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        
        # Start the WebSocket connection
        self.ws_thread = threading.Thread(target=self.ws.run_forever, kwargs={
            'ping_interval': 20,
            'ping_timeout': 10,
            'sslopt': {"cert_reqs": 0}  # Disable certificate verification for testing
        })
        self.ws_thread.daemon = True
        self.ws_thread.start()
        logger.info(f"WebSocket connection initiated to {self.url}")

    def on_open(self, ws):
        """Handle WebSocket connection open"""
        self.connected = True
        self.reconnect_count = 0
        logger.info("WebSocket connection established")
        
        # Subscribe to topics
        self.subscribe_to_topics()
        
        # Start heartbeat in a separate thread
        self.heartbeat_thread = threading.Thread(target=self.heartbeat)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()

    def subscribe_to_topics(self):
        """Subscribe to the specified topics"""
        subscribe_data = {
            "op": "subscribe",
            "args": self.topics,
            "req_id": str(uuid.uuid4())
        }
        self.ws.send(json.dumps(subscribe_data))
        logger.info(f"Subscription request sent for topics: {self.topics}")

    def heartbeat(self):
        """Send ping to keep connection alive"""
        while self.connected:
            try:
                time.sleep(15)
                if self.connected:
                    ping_msg = json.dumps({"op": "ping"})
                    self.ws.send(ping_msg)
                    logger.debug("Ping sent")
            except Exception as e:
                logger.error(f"Error in heartbeat: {e}")
                if not self.connected:
                    break

    def on_message(self, ws, message):
        """Handle incoming WebSocket messages"""
        try:
            data = json.loads(message)
            
            # Debug: Uncomment to see raw messages
            # logger.debug(f"Raw message: {json.dumps(data, indent=2)}")
            
            # Handle ping/pong for keeping connection alive
            if "op" in data:
                if data["op"] == "pong":
                    logger.debug("Received pong")
                    return
                elif data["op"] == "ping":
                    pong_msg = json.dumps({"op": "pong"})
                    ws.send(pong_msg)
                    logger.debug("Received ping, sent pong")
                    return
            
            # Handle subscription confirmation
            if "op" in data and data["op"] == "subscribe":
                if "success" in data and data["success"]:
                    logger.info(f"Successfully subscribed to {data.get('req_id', 'unknown')}")
                else:
                    logger.warning(f"Failed to subscribe: {data.get('ret_msg', 'Unknown error')}")
                return
            
            # Process order book data
            if "topic" in data and data["topic"] in self.topics:
                self.process_orderbook(data)
        
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            logger.debug(f"Message was: {message[:200]}...")  # Print first 200 chars of message

    def process_orderbook(self, data):
        """Process and display order book data"""
        try:
            logger.info("\n--- Order Book Update ---")
            
            # Check if data field exists
            if "data" not in data:
                logger.error("Error: 'data' field not found in message")
                logger.debug(f"Message content: {data}")
                return
                
            orderbook = data["data"]
            
            # Check if we're dealing with a snapshot or delta update
            update_id = orderbook.get("s", "N/A")  # s is sequence number
            logger.info(f"Update ID: {update_id}")
            
            # Extract timestamp if available
            if "ts" in orderbook:
                logger.info(f"Timestamp: {orderbook['ts']}")
            
            # Process bids (buy orders)
            if "b" in orderbook and orderbook["b"]:
                logger.info("\nBids (Buy Orders):")
                for i, bid in enumerate(orderbook["b"][:5]):  # Show top 5 bids
                    if isinstance(bid, list) and len(bid) >= 2:
                        price, size = bid[0], bid[1]
                        logger.info(f"  {i+1}. Price: {price}, Size: {size}")
            
            # Process asks (sell orders)
            if "a" in orderbook and orderbook["a"]:
                logger.info("\nAsks (Sell Orders):")
                for i, ask in enumerate(orderbook["a"][:5]):  # Show top 5 asks
                    if isinstance(ask, list) and len(ask) >= 2:
                        price, size = ask[0], ask[1]
                        logger.info(f"  {i+1}. Price: {price}, Size: {size}")
        
        except Exception as e:
            logger.error(f"Error processing orderbook data: {e}")
            logger.debug(f"Data: {data}")

    def on_error(self, ws, error):
        """Handle WebSocket errors"""
        logger.error(f"WebSocket error: {error}")
        self.connected = False

    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket connection close"""
        logger.warning(f"WebSocket connection closed: {close_msg} (Code: {close_status_code})")
        self.connected = False
        
        # Attempt to reconnect
        if self.reconnect_count < self.max_reconnects:
            self.reconnect_count += 1
            reconnect_wait = self.reconnect_interval * self.reconnect_count
            logger.info(f"Attempting to reconnect in {reconnect_wait} seconds (attempt {self.reconnect_count})")
            time.sleep(reconnect_wait)
            self.connect()
        else:
            logger.error(f"Max reconnection attempts ({self.max_reconnects}) reached. Giving up.")

    def close(self):
        """Close the WebSocket connection"""
        if self.connected:
            self.connected = False
            if self.ws:
                self.ws.close()
            logger.info("WebSocket connection closed")

def main():
    logger.info("Starting Bybit WebSocket client for BTCUSDT order book...")
    
    # Create WebSocket client instance
    client = BybitWebSocket(ws_url, topic)
    
    # Connect to WebSocket
    client.connect()
    
    try:
        # Keep the main thread running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, exiting...")
        client.close()

if __name__ == "__main__":
    main()