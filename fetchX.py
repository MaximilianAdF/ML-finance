import os
import requests
from dotenv import load_dotenv
from google.cloud import bigquery
from datetime import datetime, timedelta

load_dotenv()

# Load Google Cloud credentials dynamically
GOOGLE_CREDENTIALS_PATH = "credentials.json"
if os.path.exists(GOOGLE_CREDENTIALS_PATH):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_CREDENTIALS_PATH
else:
    raise ValueError("❌ Google Cloud credentials file not found!")
client = bigquery.Client()
dataset_id = "ml-finance-454213.x_analysis"
x_tweets = f"{dataset_id}.x_tweets"


# The date to start the query from (fetchQuery only), one day before the run
QUERY_DATE = (datetime.utcnow() - timedelta(hours=12)).strftime("%Y-%m-%d_%H:%M:%S_UTC")
BASE_URL = "https://api.twitterapi.io/twitter/"
HEADERS = {
    "X-API-Key": os.getenv("TWITTER_API_KEY"),
}

# The queries to run 
QUERIES = {
    "AAPL": ["Apple stock","AAPL stock","Apple bear","AAPL bear","Apple bull","AAPL bull","Apple drop","AAPL drop","Apple news","AAPL news","Apple earnings","AAPL earnings","Apple price target","AAPL forecast","AAPL price","AAPL update","AAPL prediction","AAPL rise","AAPL surge","AAPL rally","Apple revenue","Apple valuation","Apple performance","Apple innovation","iPhone sales","Apple services","Apple stock analysis","AAPL volatility","Apple stock predictions","Apple quarterly report","AAPL earnings call"],
    "MSFT": ["Microsoft stock", "MSFT stock", "Microsoft bear", "MSFT bear", "Microsoft bull", "MSFT bull", "Microsoft drop", "MSFT drop", "Microsoft news", "MSFT news", "Microsoft earnings", "MSFT earnings", "Microsoft price target", "MSFT forecast", "MSFT price", "Microsoft update", "MSFT update", "MSFT prediction", "Microsoft rise", "MSFT surge", "Microsoft revenue", "Microsoft quarterly report", "Microsoft products", "Microsoft services", "MSFT stock predictions", "Microsoft performance", "Microsoft AI", "MSFT earnings call", "Microsoft cloud"],
    "TSLA": ["Telsa stock", "TSLA stock", "Tesla bear", "TSLA bear", "Tesla bull", "TSLA bull", "Tesla drop", "TSLA drop", "Tesla news", "TSLA news", "Tesla earnings", "TSLA earnings", "Tesla price target", "TSLA forecast", "TSLA price", "Tesla update", "TSLA update", "TSLA prediction", "Tesla rise", "TSLA surge", "Tesla revenue", "Tesla quarterly report", "Tesla Model S", "TSLA stock predictions", "Elon Musk Tesla", "Tesla performance", "Tesla AI", "TSLA earnings call", "Tesla energy"],
    "GOOGL": ["Google stock", "GOOGL stock", "Google bear", "GOOGL bear", "Google bull", "GOOGL bull", "Google drop", "GOOGL drop", "Google news", "GOOGL news", "Google earnings", "GOOGL earnings", "Google price target", "GOOGL forecast", "GOOGL price", "Google update", "GOOGL update", "GOOGL prediction", "Google rise", "GOOGL surge", "Google revenue", "Google quarterly report", "Google cloud", "Alphabet stock", "Google AI", "GOOGL stock predictions", "Google performance", "GOOGL earnings call"],
    "AMZN": ["Amazon stock", "AMZN stock", "Amazon bear", "AMZN bear", "Amazon bull", "AMZN bull", "Amazon drop", "AMZN drop", "Amazon news", "AMZN news", "Amazon earnings", "AMZN earnings", "Amazon price target", "AMZN forecast", "AMZN price", "Amazon update", "AMZN update", "AMZN prediction", "Amazon rise", "AMZN surge", "Amazon revenue", "Amazon quarterly report", "Amazon Prime Day", "AMZN cloud", "Amazon stock predictions", "Amazon performance", "AMZN earnings call"],
}


def fetchUserTweets(user_id, cursor=""):
    """
    Fetch the last tweets of a user
    :param user_id: The Twitter ID of the user
    :param cursor: The cursor for pagination (optional)
    :return: A dictionary with the tweets and the next cursor
    """
    url = f"{BASE_URL}user/last_tweets"
    params = {
        "userId": user_id,
        "cursor": cursor, # Optional, for pagination
    }

    response = requests.get(url, headers=HEADERS, params=params)
    if response.status_code == 200:
        response = response.json()
        if response.get("has_next_page"):
            return {
                "tweets": response.get("data").get("tweets"),
                "next_cursor": response.get("next_cursor"),
            }
        else: # No more pages
            return {
                "tweets": response.get("data").get("tweets"),
                "next_cursor": None,
            }
    else:
        raise Exception(f"Error fetching user tweets: {response.status_code} - {response.text}")


def fetchQueryTweets(query, queryType, cursor=""):
    """
    Fetch tweets based on a query
    :param query: The query to search for
    :param queryType: The type of query ("Latest" or "Top")
    :param cursor: The cursor for pagination (optional)
    :return: A dictionary with the tweets and the next cursor
    """
    if queryType not in ["Latest", "Top"]:
        raise ValueError("queryType must be either 'Latest' or 'Top'")

    url = f"{BASE_URL}tweet/advanced_search"
    params = {
        "query": query, #-filter:nativeretweets -filter:retweets -filter:links
        "queryType": queryType,
        "cursor": cursor, # Optional, for pagination
    }


    response = requests.get(url, headers=HEADERS, params=params)
    if response.status_code == 200:
        response = response.json()
        if response.get("has_next_page"):
            return {
                "tweets": response.get("tweets"),
                "next_cursor": response.get("next_cursor"),
            }
        else: # No more pages
            return {
                "tweets": response.get("tweets"),
                "next_cursor": None,
            }
    else:
        raise Exception(f"Error fetching tweets: {response.status_code} - {response.text}")


def filterTweets(tweets, ticker):
    """
    Filter the tweets to only include the necessary and wanted fields
    :param tweets: The list of tweets
    :return: A list of filtered tweets for params
    """
    filtered_tweets = []
    for tweet in tweets:

        filtered_tweet = {
            "id": tweet.get("id"),
            "created_utc": datetime.strptime(tweet.get("createdAt"), "%a %b %d %H:%M:%S +0000 %Y").strftime("%Y-%m-%d %H:%M:%S UTC"),
            "text": tweet.get("text"),
            "likes": tweet.get("likeCount"),
            "retweets": tweet.get("retweetCount"),
            "replies": tweet.get("replyCount"),
            "views": tweet.get("viewCount"),
            "ticker": ticker
        }

        filtered_tweets.append(filtered_tweet)
    return filtered_tweets


def postToBigQuery(filtered_tweets):
    """
    Post the filtered tweets to BigQuery
    :param filtered_tweets: The list of filtered tweets
    :return: None
    """
    try:
        job = client.insert_rows_json(x_tweets, filtered_tweets)
        if job == []:
            print("✅ Tweets Saved to BigQuery!")
        else:
            print("❌ Error inserting tweets:", job)
    except Exception as e:
        print("❌ Error inserting tweets:", e)
    

def buildQuery(company_keywords):
    """
    Build the query for the Twitter API
    :param company_keywords: The keywords to search for
    :return: The query string
    """
    return " OR ".join(company_keywords)


def automateBiDaily():
    """
    Automate the process of fetching tweets and posting to BigQuery every two hours
    :return: None
    """
    for ticker, keywords in QUERIES.items():
        query = f"since:{QUERY_DATE} {buildQuery(keywords)}"

        pages = 0
        cursor = ""
        while (cursor != None and pages < 15):
            response = fetchQueryTweets(query, "Top", cursor)
            tweets = response.get("tweets")
            cursor = response.get("next_cursor")
            
            if tweets:
                filtered_tweets = filterTweets(tweets, ticker)
                postToBigQuery(filtered_tweets)

            else:
                print(f"❌ No tweets found for {ticker} page {pages}")
                break

            pages += 1

        print(f"✅ Finished fetching tweets for {ticker}, {pages} page(s) fetched")

            

automateBiDaily()