import csv
import os
import uuid
from collections import defaultdict
from datetime import datetime, timezone

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer

DATA_DIR = "/usr/local/airflow/include/data"
ARTICLES_KEYWORDS_CSV = os.path.join(DATA_DIR, "nyt_data/articles_keywords.csv")
EVENTS_KEYWORDS_CSV = os.path.join(DATA_DIR, "kalshi_data/events_keywords.csv")
ARTICLES_CSV = os.path.join(DATA_DIR, "nyt_data/nyt_articles.csv")
EVENTS_CSV = os.path.join(DATA_DIR, "kalshi_data/kalshi_events.csv")
MATCHES_CSV = os.path.join(DATA_DIR, "matches.csv")


def load_junction(path: str, key_field: str) -> dict[str, list[str]]:
    out: dict[str, list[str]] = defaultdict(list)
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            out[row[key_field]].append(row["keyword_id"])
    return out


def compute_matches(min_score: float = 0.0) -> str:
    articles = load_junction(ARTICLES_KEYWORDS_CSV, "article_id")
    events = load_junction(EVENTS_KEYWORDS_CSV, "event_id")
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")

    with open(MATCHES_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["ID", "event_id", "article_id", "score", "timestamp"]
        )
        writer.writeheader()

        if not articles or not events:
            return MATCHES_CSV

        article_ids = list(articles.keys())
        docs = [articles[a] for a in article_ids]

        vectorizer = TfidfVectorizer(
            analyzer=lambda x: x, lowercase=False, token_pattern=None
        )
        matrix = vectorizer.fit_transform(docs)
        vocab = vectorizer.vocabulary_

        for event_id, event_kws in events.items():
            cols = [vocab[k] for k in set(event_kws) if k in vocab]
            if not cols:
                continue
            scores = np.asarray(matrix[:, cols].sum(axis=1)).ravel()
            for i, total in enumerate(scores):
                total = float(total)
                if total <= min_score:
                    continue
                writer.writerow({
                    "ID": str(uuid.uuid4()),
                    "event_id": event_id,
                    "article_id": article_ids[i],
                    "score": total,
                    "timestamp": timestamp,
                })
    return MATCHES_CSV


def load_titles(path: str) -> dict[str, str]:
    titles: dict[str, str] = {}
    if not os.path.exists(path):
        return titles
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            titles[row["ID"]] = row.get("title", "")
    return titles


def print_top_matches(n: int = 5) -> None:
    rows: list[dict] = []
    with open(MATCHES_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            row["score"] = float(row["score"])
            rows.append(row)
    rows.sort(key=lambda r: r["score"], reverse=True)
    top = rows[:n]

    event_titles = load_titles(EVENTS_CSV)
    article_titles = load_titles(ARTICLES_CSV)

    print("-------------------------------------------")
    print(f"  Top {len(top)} matches (event ↔ article)")
    print("-------------------------------------------")
    for i, r in enumerate(top, start=1):
        ev_title = event_titles.get(r["event_id"], "")
        ar_title = article_titles.get(r["article_id"], "")
        print()
        print(f"  [{i}] score: {r['score']:.4f}")
        print(f"      event    {r['event_id']}")
        print(f"               \"{ev_title}\"")
        print(f"      article  {r['article_id']}")
        print(f"               \"{ar_title}\"")
    print("-------------------------------------------")
