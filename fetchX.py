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
QUERIES = [
    "Federal Reserve OR interest rates OR inflation OR recession OR economic growth OR GDP OR CPI OR PPI "
    "OR stock market crash OR stock market rally OR Wall Street reaction",

    "bought stock OR sold stock OR investing in OR stock went up OR stock went down "
    "OR stock trading OR buying stocks OR stock prediction",

    # 3️⃣ Hashtags & Market Sentiment
    "#StockMarket OR #Stocks OR #Investing OR #OptionsTrading OR #Earnings OR #WallStreet "
    "#Bullish OR #Bearish OR #MarketCrash OR #Recession OR #Inflation OR #FederalReserve",
    
    # 4️⃣ General Stock Market & Economic Keywords
    "stock market OR stocks OR trading OR investing OR Wall Street OR bull market OR bull OR bear OR bear market "
    "OR economic crash OR interest rates OR Federal Reserve OR rate hike OR GDP OR CPI OR PPI "
    "OR earnings report OR inflation data OR bond yields OR market volatility",
    
    # 5️⃣ Financial News Sources
    "from:CNBC OR from:WSJMarkets OR from:Bloomberg OR from:FinancialTimes OR from:ReutersBiz "
    "OR from:Stocktwits OR from:zerohedge OR from:business OR from:realDonaldTrump"
]


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


def filterTweets(tweets):
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
            "sentiment": [],
            "market_reference": [],
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
    


def automateBiDaily():
    """
    Automate the process of fetching tweets and posting to BigQuery every two hours
    :return: None
    """
    for query in QUERIES:
        query += f" since:{QUERY_DATE}"

        pages = 0
        cursor = ""
        while (cursor != None and pages < 1):
            response = fetchQueryTweets(query, "Top", cursor)
            tweets = response.get("tweets")
            cursor = response.get("next_cursor")
            
            if tweets:
                filtered_tweets = filterTweets(tweets)
                postToBigQuery(filtered_tweets)

            else:
                print(f"❌ No tweets found for {query} page {pages + 1}")
                break

            pages += 1

        print(f"✅ Finished fetching tweets for {query}, {pages} pages fetched")

            

automateBiDaily()