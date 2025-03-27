import os
import praw
import json
import uuid
from datetime import datetime, timezone
from google.cloud import bigquery
from dotenv import load_dotenv

# Constants
POSTS_TO_FETCH = 1000
COMMENTS_PER_POST = 1000

# Load environment variables
load_dotenv()

# Load Google Cloud credentials dynamically
GOOGLE_CREDENTIALS_PATH = "credentials.json"
if os.path.exists(GOOGLE_CREDENTIALS_PATH):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_CREDENTIALS_PATH
else:
    raise ValueError("❌ Google Cloud credentials file not found!")

# Initialize BigQuery client
client = bigquery.Client()
dataset_id = "ml-finance-454213.wsb_analysis"

# Define BigQuery tables
posts_table = f"{dataset_id}.wsb_posts"
comments_table = f"{dataset_id}.wsb_comments"

# ✅ Reddit API Credentials (Replace with your credentials)
reddit = praw.Reddit(
    client_id=os.getenv("CLIENT_ID"),
    client_secret=os.getenv("CLIENT_SECRET"),
    user_agent="your_user_agent"
)

# ✅ Fetch posts from WallStreetBets
subreddit = reddit.subreddit("wallstreetbets")
posts_data = [] 

for post in subreddit.new(limit=POSTS_TO_FETCH):

    post_dict = {
        "id": post.id,
        "title": post.title,
        "body": post.selftext,
        "upvotes": post.score,
        "comments_count": post.num_comments,
        "created_utc": datetime.utcfromtimestamp(post.created_utc).replace(tzinfo=timezone.utc).isoformat(),
        "url": f"https://www.reddit.com{post.permalink}",
        "image_url": post.url if post.url.endswith((".jpg", ".png", ".gif")) else None,
        "sentiment": [],
        "market_reference": []
    }
    posts_data.append(post_dict)

# ✅ Insert posts into BigQuery
job = client.insert_rows_json(posts_table, posts_data)
if job == []:
    print("✅ WSB Posts Saved to BigQuery!")
else:
    print("❌ Error inserting posts:", job)

# ✅ Fetch comments for each post
comments_data = []
for post in posts_data:
    submission = reddit.submission(id=post["id"])
    submission.comments.replace_more(limit=5)
    
    comment_count = 0  # Track comment count
    print(f"Fetching comments for post: {submission.title} - {len(submission.comments.list())}")
    for comment in submission.comments.list():
        comment_dict = {
            "id": comment.id,
            "post_id": post["id"],
            "body": comment.body,
            "upvotes": comment.score,
            "created_utc": datetime.utcfromtimestamp(comment.created_utc).replace(tzinfo=timezone.utc).isoformat(),
            "sentiment": [],
            "market_reference": [],
            "agrees_with_post": []
        }
        comments_data.append(comment_dict)

# ✅ Insert comments into BigQuery
job = client.insert_rows_json(comments_table, comments_data)
if job == []:
    print("✅ WSB Comments Saved to BigQuery!")
else:
    print("❌ Error inserting comments:", job)
