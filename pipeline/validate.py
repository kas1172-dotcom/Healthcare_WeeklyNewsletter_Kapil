#!/usr/bin/env python3
"""Validate Healthcare Regulatory & Policy Monitor raw and newsletter JSON output."""

import argparse
import json
import os
import re
from urllib.parse import urlparse

REQUIRED_RAW_FIELDS = [
    "title", "url", "published", "published_iso", "source_name",
    "source_url", "snippet", "doc_type", "comment_deadline", "source_type"
]

REQUIRED_NEWSLETTER_FIELDS = [
    "subject_line", "theme_of_week", "editors_note", "categories", "articles"
]

REQUIRED_ARTICLE_FIELDS = [
    "title", "url", "published", "source_name", "source_url",
    "snippet", "doc_type", "comment_deadline", "source_type",
    "category", "urgency", "headline", "summary", "implication", "is_comment_period",
    # Added in the analysis/classification rewrite — keep in sync with
    # generate.py:validate_article_output()
    "importance_score", "consulting_relevance", "policy_relevance",
    "so_what", "now_what",
]

# Fields that must be numbers in 0–100 (mirrors generate.py)
SCORE_FIELDS = ["importance_score", "consulting_relevance", "policy_relevance"]

VALID_URGENCY = {"urgent", "important", "routine", "discard"}

URL_RE = re.compile(r"^https?://")


def is_valid_url(value):
    return isinstance(value, str) and URL_RE.match(value)


def load_json(path):
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def validate_raw(path):
    data = load_json(path)
    if not isinstance(data, dict):
        raise ValueError("raw JSON must be an object")
    articles = data.get("articles")
    if not isinstance(articles, list):
        raise ValueError("raw JSON missing articles list")
    for idx, article in enumerate(articles):
        if not isinstance(article, dict):
            raise ValueError(f"raw article {idx} is not an object")
        for field in REQUIRED_RAW_FIELDS:
            if field not in article:
                raise ValueError(f"raw article {idx} missing field {field}")
        if not is_valid_url(article["url"]):
            raise ValueError(f"raw article {idx} has invalid URL: {article['url']}")
    print(f"✓ raw JSON valid: {len(articles)} articles")


def validate_newsletter(path):
    data = load_json(path)
    if not isinstance(data, dict):
        raise ValueError("newsletter JSON must be an object")
    for field in ("generated_at", "week_of", "consulting", "policy"):
        if field not in data:
            raise ValueError(f"newsletter missing {field}")
    for edition in ("consulting", "policy"):
        edition_data = data[edition]
        if not isinstance(edition_data, dict):
            raise ValueError(f"{edition} section must be an object")
        for field in REQUIRED_NEWSLETTER_FIELDS:
            if field not in edition_data:
                raise ValueError(f"{edition} edition missing {field}")
        if not isinstance(edition_data["categories"], list):
            raise ValueError(f"{edition}.categories must be a list")
        if not isinstance(edition_data["articles"], list):
            raise ValueError(f"{edition}.articles must be a list")
        for idx, article in enumerate(edition_data["articles"]):
            if not isinstance(article, dict):
                raise ValueError(f"{edition} article {idx} is not an object")
            for field in REQUIRED_ARTICLE_FIELDS:
                if field not in article:
                    raise ValueError(f"{edition} article {idx} missing {field}")
            if not is_valid_url(article["url"]):
                raise ValueError(f"{edition} article {idx} has invalid URL: {article['url']}")
            if article["urgency"] not in VALID_URGENCY:
                raise ValueError(f"{edition} article {idx} invalid urgency: {article['urgency']}")
            for field in SCORE_FIELDS:
                val = article[field]
                if not isinstance(val, (int, float)) or isinstance(val, bool) or not (0 <= val <= 100):
                    raise ValueError(f"{edition} article {idx} {field} must be a number 0–100, got {val!r}")
    print(f"✓ newsletter JSON valid: {data['consulting']['articles'].__len__()} consulting articles, {data['policy']['articles'].__len__()} policy articles")


def main():
    parser = argparse.ArgumentParser(description="Validate Healthcare Regulatory & Policy Monitor JSON outputs")
    parser.add_argument("--raw", default="raw_articles.json", help="raw articles JSON file")
    parser.add_argument("--newsletter", default="newsletter_data.json", help="newsletter JSON file")
    args = parser.parse_args()

    validate_raw(args.raw)
    validate_newsletter(args.newsletter)
    print("All validations passed.")


if __name__ == "__main__":
    main()
