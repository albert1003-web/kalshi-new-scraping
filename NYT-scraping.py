import argparse
import csv
import re
import requests
from datetime import datetime, timezone, timedelta
from rake_nltk import Rake

# Usage: python nyt.py [-m MINUTES] [-o OUTPUT_PATH] [-r]
#   -m  How many minutes back to fetch articles (default: 10)
#   -o  Output CSV file path (default: nyt_articles.csv)
#   -r  Strip punctuation from extracted keywords

NYT_API_KEY = "H6qXc3zNyk2hI38RErm60uhEAMP8KTT7y5ugRV6jK2oVH4Gy"  # https://developer.nytimes.com/

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


def clean_uri(uri: str) -> str:
    return uri.replace("nyt://article/", "")


def format_published_date(date_str: str) -> str:
    if not date_str:
        return ""
    dt = datetime.fromisoformat(date_str)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")


def get_recent_articles(minutes: int = 10, source: str = "all", section: str = "all") -> list[dict]:
    url = f"https://api.nytimes.com/svc/news/v3/content/{source}/{section}.json"
    params = {
        "api-key": NYT_API_KEY,
        "limit": 500,
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    all_articles = response.json().get("results", [])

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    return [
        a for a in all_articles
        if datetime.fromisoformat(a["published_date"]) >= cutoff
    ]


def save_to_csv(
    articles: list[dict],
    output_path: str = "nyt_data/nyt_articles.csv",
    remove_punctuation: bool = False,
) -> str:
    fieldnames = ["ID", "title", "abstract", "key_words", "publish_time"]

    with open("nyt_data/output_path", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for article in articles:
            uri      = clean_uri(article.get("uri", ""))
            title    = article.get("title", "")
            abstract = article.get("abstract", "")
            keywords = extract_keywords(f"{title}. {abstract}", remove_punctuation)
            pub_time = format_published_date(article.get("published_date", ""))

            writer.writerow({
                "ID":           uri,
                "title":        title,
                "abstract":     abstract,
                "key_words":    keywords,
                "publish_time": pub_time,
            })

    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch recent NYT articles via the Times Newswire API and save to CSV."
    )
    parser.add_argument(
        "--minutes", "-m",
        type=int,
        default=10,
        help="How many minutes back to fetch articles (default: 10)",
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default="nyt_articles.csv",
        help="Output CSV file path (default: nyt_articles.csv)",
    )
    parser.add_argument(
        "--remove-punctuation", "-r",
        action="store_true",
        default=False,
        help="Strip punctuation from extracted keywords (default: False)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    articles = get_recent_articles(minutes=args.minutes)
    path = save_to_csv(articles, output_path=args.output, remove_punctuation=args.remove_punctuation)
    print(f"Saved {len(articles)} article(s) to '{path}'")