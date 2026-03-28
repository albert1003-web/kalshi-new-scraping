import argparse
import csv
import re
import requests
from rake_nltk import Rake
import nltk

try:
    nltk.data.find('corpora/stopwords')
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('stopwords')
    nltk.download('punkt')

KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"

def extract_keywords(text: str, remove_punctuation: bool = False) -> str:
    if not text or not text.strip():
        return ""
    rake = Rake()
    rake.extract_keywords_from_text(text)
    phrases = rake.get_ranked_phrases()
    if remove_punctuation:
        phrases = [re.sub(r'[^\w\s]', '', phrase) for phrase in phrases]
    seen = set()
    unique_phrases = []
    for phrase in phrases:
        normalised = phrase.lower().strip()
        if normalised not in seen:
            seen.add(normalised)
            unique_phrases.append(phrase)
    return " | ".join(unique_phrases[:10])

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

def save_to_csv(events: list[dict], output_path: str, remove_punc: bool):
    fieldnames = ["ID", "title", "submarket", "yes_price", "no_price", "key_words"]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        count = 0
        for event in events:
            markets = event.get("markets")
            
            if markets: # explode submarkets
                for mkt in markets:
                    title = mkt.get("title", "")
                    subtitle = mkt.get("yes_sub_title", "")
                    keywords = extract_keywords(f"{title} {subtitle}", remove_punc)
                    writer.writerow({
                        "ID":           mkt.get("ticker", ""),
                        "title":        title,
                        "submarket":    subtitle,
                        "yes_price":    mkt.get("yes_ask_dollars", 0),
                        "no_price":     mkt.get("no_ask_dollars", 0),
                        "key_words":    keywords,
                    })
                    count += 1

    return count

def parse_args():
    parser = argparse.ArgumentParser(description="Fetch Kalshi events to CSV.")
    parser.add_argument("--output", "-o", default="kalshi_events.csv")
    parser.add_argument("--remove-punctuation", "-r", action="store_true")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    print("Fetching events from Kalshi...")
    try:
        events_data = get_all_kalshi_events()
        total_rows = save_to_csv(events_data, f"kalshi_data/{args.output}", args.remove_punctuation)
        print(f"Successfully saved {total_rows} rows to '{args.output}'")
    except Exception as e:
        print(f"An error occurred: {e}")