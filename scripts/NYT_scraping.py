import argparse
import csv
import re
import requests
from datetime import datetime, timezone, timedelta
from rake_nltk import Rake
import os

# Usage: python nyt.py [-m MINUTES] [-o OUTPUT_PATH] [-r]
#   -m  How many minutes back to fetch articles (default: 10)
#   -o  Output CSV file path (default: nyt_articles.csv)

NYT_API_KEY = "H6qXc3zNyk2hI38RErm60uhEAMP8KTT7y5ugRV6jK2oVH4Gy"  # https://developer.nytimes.com/

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
    # output_path: str = "nyt_data/nyt_articles.csv",
    remove_punctuation: bool = False,
) -> str:
    base_dir = "/usr/local/airflow/include/data/nyt_data"
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
        
    articles_file = os.path.join(base_dir, "nyt_articles.csv")
    keywords_file = os.path.join("/usr/local/airflow/include/data/keywords.csv")
    junction_file = os.path.join(base_dir, "articles_keywords.csv")

    master_keywords = set() # unique keywords
    keywords_exists = os.path.exists(keywords_file)

    with open(articles_file, "w", newline="", encoding="utf-8") as f_art, \
         open(keywords_file, "a", newline="", encoding="utf-8") as f_key, \
         open(junction_file, "w", newline="", encoding="utf-8") as f_jun:

        art_writer = csv.DictWriter(f_art, fieldnames=["ID", "title", "abstract", "publish_time"])
        key_writer = csv.DictWriter(f_key, fieldnames=["ID", "word"])
        jun_writer = csv.DictWriter(f_jun, fieldnames=["article_id", "keyword_id"])

        art_writer.writeheader()
        if not keywords_exists:
            key_writer.writeheader()
        jun_writer.writeheader()

        for article in articles:
            uri      = clean_uri(article.get("uri", ""))
            title    = article.get("title", "")
            abstract = article.get("abstract", "")
            pub_time = format_published_date(article.get("published_date", ""))
            
            # Get the list of keywords
            kw_list = extract_keywords(f"{title}. {abstract}", remove_punctuation)

            # Write to ny_times_articles.csv
            art_writer.writerow({
                "ID": uri,
                "title": title,
                "abstract": abstract,
                "publish_time": pub_time
            })

            # Write to keywords.csv and articles_keywords.csv
            for kw in kw_list:
                # Add to junction table
                jun_writer.writerow({
                    "article_id": uri,
                    "keyword_id": kw
                })
                # Collect for master keyword list
                master_keywords.add(kw)

        # Finally, write the unique master keywords
        for kw in sorted(list(master_keywords)):
            key_writer.writerow({"ID": kw, "word": kw})

    return articles_file


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
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    articles = get_recent_articles(minutes=args.minutes)
    path = save_to_csv(articles, remove_punctuation=True)
    # path = save_to_csv(articles, output_path=args.output, remove_punctuation=args.remove_punctuation)
    print(f"Saved {len(articles)} article(s) to '{path}'")