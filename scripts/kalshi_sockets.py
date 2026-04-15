import asyncio
import json
import os
import time
import base64
import sys
from confluent_kafka import Producer
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
import websockets
from dotenv import load_dotenv

# usage: python kalshi_sockets.py KXGOVTSHUTLENGTH-26FEB07-G60

class Colors:
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    CYAN = '\033[96m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    END = '\033[0m'

load_dotenv()

API_KEY_ID = os.getenv("KALSHI_API_KEY_ID")
PRIVATE_KEY_PATH = "../5114_key.txt"
WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"

conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'kalshi-producer'
}
producer = Producer(conf)

def delivery_report(err, msg):
    """ Optional callback to confirm the message hit the broker """
    if err is not None:
        print(f'Kafka Delivery failed: {err}')

def sign_wss_handshake(private_key_path):
    with open(private_key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(), password=None
        )

    timestamp = str(int(time.time() * 1000))
    msg = f"{timestamp}GET/trade-api/ws/v2"
    
    signature = private_key.sign(
        msg.encode('utf-8'),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )
    
    return {
        "KALSHI-ACCESS-KEY": API_KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode('utf-8'),
        "KALSHI-ACCESS-TIMESTAMP": timestamp
    }

async def subscribe_kalshi(ticker):
    headers = sign_wss_handshake(PRIVATE_KEY_PATH)
    
    async with websockets.connect(WS_URL, additional_headers=headers) as ws:
        print(f"Subscribed to ticker {ticker}")

        subscribe_msg = {
            "id": 1,
            "cmd": "subscribe",
            "params": {
                "channels": ["ticker", "trade", "orderbook_delta"],
                "market_ticker": ticker
            }
        }

        await ws.send(json.dumps(subscribe_msg))

        async for message in ws:
            # send to kafka
            producer.produce(
                topic='kalshi_market_data', 
                key=market_ticker, 
                value=message, 
                on_delivery=delivery_report
            )
            producer.poll(0)

            data = json.loads(message)
            msg_type = data.get("type")
            msg_content = data.get("msg", {})

            if msg_type == "orderbook_delta":
                ticker = msg_content.get("market_ticker")
                price_str = msg_content.get("price_dollars")
                price_val = float(price_str)
                side = msg_content.get("side", "").upper()
                delta = msg_content.get("delta_fp")
                ts = msg_content.get("ts")

                implied_price = 1.0 - price_val
                implied_side = "YES" if side == "NO" else "NO"
                
                action_color = Colors.GREEN if float(delta) > 0 else Colors.RED
                action_text = "Volume +" if float(delta) > 0 else "Volume"
                
                print(f"{Colors.YELLOW}[DELTA]{Colors.END} {ticker} | "
                    f"{Colors.CYAN}{side} @ ${price_val:.4f}{Colors.END} ({implied_side} Implied: ${implied_price:.4f}) | "
                    f"{action_color}{action_text} {delta}{Colors.END} | {ts}")

            elif msg_type == "ticker":
                ticker_name = msg_content.get('market_ticker')
                yes_bid = msg_content.get('yes_bid_dollars')
                yes_ask = msg_content.get('yes_ask_dollars')
                
                no_bid = f"{1.0 - float(yes_ask):.4f}" if yes_ask else "None"
                no_ask = f"{1.0 - float(yes_bid):.4f}" if yes_bid else "None"

                print(f"{Colors.BLUE}[TICKER]{Colors.END} {ticker_name} | "
                    f"Yes Bid: {yes_bid} | Yes Ask: {yes_ask} | "
                    f"No Bid: {no_bid} | No Ask: {no_ask}")

            elif msg_type == "subscribed":
                print(f"{Colors.GREEN}Subscribed to {msg_content.get('channel')}{Colors.END}")
            
            elif msg_type == "orderbook_snapshot":
                print(f"{Colors.BLUE}{Colors.BOLD}--- Orderbook Snapshot received for {ticker} ---{Colors.END}")

            elif msg_type == "trade":
                ticker_name = msg_content.get('market_ticker')
                side = msg_content.get('taker_side', 'unknown').upper()
                price = msg_content.get('yes_price_dollars', '0.00')
                no_price = msg_content.get('no_price_dollars', '0.00')
                size = int(float(msg_content.get('count_fp', 0)))
                
                yes_display = f"{Colors.GREEN}Yes Price: {price}{Colors.END}" if side == "YES" else f"Yes Price: {price}"
                no_display = f"{Colors.GREEN}No Price: {no_price}{Colors.END}" if side == "NO" else f"No Price: {no_price}"

                print(f"{Colors.GREEN}{Colors.BOLD}[TRADE]{Colors.END}  {ticker_name} | "
                    f"Side: {side} | Size: {size} | {Colors.GREEN}Total Price: ${float(price) * size:.4f}{Colors.END} | "
                    f"{yes_display} | {no_display}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python kalshi_sockets.py <MARKET_TICKER>")
        sys.exit(1)

    market_ticker = sys.argv[1]

    try:
        asyncio.run(subscribe_kalshi(market_ticker))
    except KeyboardInterrupt:
        print("\nStopping")