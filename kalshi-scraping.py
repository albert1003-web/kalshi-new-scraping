import argparse
import csv
import re
import requests
from rake_nltk import Rake
import nltk
import os

try:
    nltk.data.find('corpora/stopwords')
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('stopwords')
    nltk.download('punkt')

KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"

def extract_keywords(text: str, remove_punctuation: bool = True) -> list:
    if not text or not text.strip():
        return []
    rake = Rake()
    rake.extract_keywords_from_text(text)
    phrases = rake.get_ranked_phrases()
    
    if remove_punctuation:
        phrases = [re.sub(r'[^\w\s]', '', phrase) for phrase in phrases]
        
    seen = set()
    unique_phrases = []
    for phrase in phrases:
        normalised = phrase.lower().strip()
        
        if normalised and normalised not in seen and not any(char.isdigit() for char in normalised):
            seen.add(normalised)
            unique_phrases.append(normalised)
            
    return unique_phrases[:10]

def get_all_kalshi_events():
    url = f"{KALSHI_API_BASE}/events"
    events = []
    cursor = ""

    while True:
        params = {
            "status": "open", 
            "cursor": cursor, 
            "with_nested_markets": True,
            "limit": 200
        }
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        new_events = data.get("events", [])
        events.extend(new_events)

        cursor = data.get("cursor")
        
        if not cursor:
            break

    return events

def save_to_csv(events: list[dict]):
    base_dir = 'data/kalshi_data'
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    # File Paths
    events_file = os.path.join(base_dir, "kalshi_events.csv")
    keywords_file = os.path.join("data/keywords.csv")
    junction_file = os.path.join(base_dir, "events_keywords.csv")

    master_keywords = set()
    
    # Open all three writers
    with open(events_file, "w", newline="", encoding="utf-8") as f_ev, \
         open(keywords_file, "a", newline="", encoding="utf-8") as f_key, \
         open(junction_file, "w", newline="", encoding="utf-8") as f_jun:

        ev_writer = csv.DictWriter(f_ev, fieldnames=["ID", "title", "submarket", "yes_price", "no_price"])
        key_writer = csv.DictWriter(f_key, fieldnames=["ID", "word"])
        jun_writer = csv.DictWriter(f_jun, fieldnames=["event_id", "keyword_id"])

        ev_writer.writeheader()
        jun_writer.writeheader()

        count = 0
        for event in events:
            markets = event.get("markets")
            if not markets:
                continue
                
            for mkt in markets:
                ticker = mkt.get("ticker", "")
                title = mkt.get("title", "")
                subtitle = mkt.get("yes_sub_title", "")
                
                # Save core event/market data
                ev_writer.writerow({
                    "ID":        ticker,
                    "title":     title,
                    "submarket": subtitle,
                    "yes_price": mkt.get("yes_ask_dollars", 0),
                    "no_price":  mkt.get("no_ask_dollars", 0),
                })

                # same logic: write to the event map table and update master keywords
                kw_list = extract_keywords(f"{title} {subtitle}", True) 
                for kw in kw_list:
                    jun_writer.writerow({
                        "event_id": ticker,
                        "keyword_id": kw
                    })
                    master_keywords.add(kw)
                
                count += 1

        # Update master keywords list
        for kw in sorted(list(master_keywords)):
            key_writer.writerow({"ID": kw, "word": kw})

    return count

def parse_args():
    parser = argparse.ArgumentParser(description="Fetch Kalshi events to CSV.")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    print("Fetching events from Kalshi...")
    try:
        events_data = get_all_kalshi_events()
        total_rows = save_to_csv(events_data)
        print(f"Successfully saved {total_rows} rows")
    except Exception as e:
        print(f"An error occurred: {e}")