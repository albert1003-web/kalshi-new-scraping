import tkinter as tk
import threading
import json
import time
from confluent_kafka import Consumer

class KalshiDualProPulse:
    def __init__(self, root):
        self.root = root
        self.root.title("Kalshi Event Watcher")
        self.root.geometry("800x600")
        self.root.configure(bg="#050505")

        # Market State
        self.yes_total = 0.0
        self.no_total = 0.0
        self.yes_bid = 0.0
        self.yes_ask = 0.0
        self.no_bid = 0.0
        self.no_ask = 0.0
        
        # Delta History (Last 5 per side)
        self.yes_deltas = [] 
        self.no_deltas = []  

        self.canvas = tk.Canvas(root, bg="#050505", highlightthickness=0)
        self.canvas.pack(fill=tk.BOTH, expand=True)

        self.conf = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'dual-pro-pulse-group',
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(self.conf)
        self.consumer.subscribe(['kalshi_market_data'])

        self.running = True
        threading.Thread(target=self.kafka_worker, daemon=True).start()
        self.animate()

    def kafka_worker(self):
        yes_book = {}
        no_book = {}

        while self.running:
            msg = self.consumer.poll(0.05)
            if msg is None or msg.error(): continue
            
            data = json.loads(msg.value().decode('utf-8'))
            msg_type = data.get("type")
            content = data.get("msg")

            if msg_type == "orderbook_snapshot":
                yes_book = {p: float(s) for p, s in content.get("yes_dollars_fp", [])}
                no_book = {p: float(s) for p, s in content.get("no_dollars_fp", [])}
            
            elif msg_type == "orderbook_delta":
                price = content.get("price_dollars")
                delta = float(content.get("delta_fp"))
                side = content.get("side")
                
                target_book = yes_book if side == "yes" else no_book
                target_book[price] = max(0, target_book.get(price, 0) + delta)

                target_list = self.yes_deltas if side == "yes" else self.no_deltas
                if delta != 0:
                    target_list.append(delta)
                    if len(target_list) > 5: target_list.pop(0)

            elif msg_type == "ticker":
                y_bid = float(content.get("yes_bid_dollars", 0))
                y_ask = float(content.get("yes_ask_dollars", 0))
                self.yes_bid = y_bid
                self.yes_ask = y_ask
                # Calculate NO side inversion
                self.no_bid = 1.0 - y_ask if y_ask > 0 else 0.0
                self.no_ask = 1.0 - y_bid if y_bid > 0 else 0.0

            self.yes_total = sum(yes_book.values())
            self.no_total = sum(no_book.values())

    def animate(self):
        if not self.running: return
        self.canvas.delete("all")
        w, h = self.canvas.winfo_width(), self.canvas.winfo_height()

        combined = self.yes_total + self.no_total or 1
        split_x = w * (self.no_total / combined)
        
        self.canvas.create_rectangle(0, 0, split_x, h, fill="#200000", outline="")
        self.canvas.create_rectangle(split_x, 0, w, h, fill="#002000", outline="")
        self.canvas.create_line(split_x, 0, split_x, h, fill="white", width=2)

        # 1. NO SIDE BID/ASK (Left Side)
        self.draw_quote_box(w*0.25, 45, self.no_bid, self.no_ask, "NO")

        # 2. YES SIDE BID/ASK (Right Side)
        self.draw_quote_box(w*0.75, 45, self.yes_bid, self.yes_ask, "YES")

        # 3. MAIN TOTALS
        self.canvas.create_text(w*0.25, h*0.35, text=f"NO\n{int(self.no_total)}", fill="#ff4444", font=("Arial", 60, "bold"), justify=tk.CENTER)
        self.canvas.create_text(w*0.75, h*0.35, text=f"YES\n{int(self.yes_total)}", fill="#44ff44", font=("Arial", 60, "bold"), justify=tk.CENTER)

        # 4. DELTA HISTORY
        for i, val in enumerate(reversed(self.no_deltas)):
            prefix = "+" if val > 0 else ""
            alpha = hex(max(50, 255 - (i * 45)))[2:].zfill(2)
            self.canvas.create_text(w*0.25, h*0.55 + (i * 38), text=f"{prefix}{int(val)}", fill=f"#ff{alpha}{alpha}", font=("Arial", 24, "bold"))

        for i, val in enumerate(reversed(self.yes_deltas)):
            prefix = "+" if val > 0 else ""
            alpha = hex(max(50, 255 - (i * 45)))[2:].zfill(2)
            self.canvas.create_text(w*0.75, h*0.55 + (i * 38), text=f"{prefix}{int(val)}", fill=f"#{alpha}ff{alpha}", font=("Arial", 24, "bold"))

        self.root.after(30, self.animate)

    def draw_quote_box(self, x, y, bid, ask, side):
        # Background box
        self.canvas.create_rectangle(x-80, y-30, x+80, y+30, fill="#000", outline="#444", width=1)
        # Bid (Left side of box)
        self.canvas.create_text(x-40, y-15, text="BID", fill="#666", font=("Arial", 8, "bold"))
        self.canvas.create_text(x-40, y+8, text=f"${bid:.2f}", fill="#00ff00" if side == "YES" else "#ff4444", font=("Courier", 16, "bold"))
        # Ask (Right side of box)
        self.canvas.create_text(x+40, y-15, text="ASK", fill="#666", font=("Arial", 8, "bold"))
        self.canvas.create_text(x+40, y+8, text=f"${ask:.2f}", fill="#ff4444" if side == "YES" else "#00ff00", font=("Courier", 16, "bold"))

if __name__ == "__main__":
    root = tk.Tk()
    app = KalshiDualProPulse(root)
    root.mainloop()