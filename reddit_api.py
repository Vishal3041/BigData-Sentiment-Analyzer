import sys
import pandas as pd
import praw
from praw import Reddit
import configparser


def connect_reddit(client_id, client_secret, user_agent, username, password, redirect_url) -> Reddit:
    try:
        reddit = praw.Reddit(client_id=client_id,
                             client_secret=client_secret,
                             username=username,
                             password=password,
                             user_agent=user_agent,
                             redirect_uri=redirect_url)
        print("connected to reddit!", reddit.user.me())

        return reddit
    except Exception as e:
        print(e)
        sys.exit(1)


def extract_posts(reddit_instance: Reddit, subreddit: str, time_filter: str, limit=None):
    subreddit = reddit_instance.subreddit(subreddit)
    posts = subreddit.top(time_filter=time_filter, limit=limit)

    POST_FIELDS = (
        'title',
        'score',
        'num_comments',
        'author',
        'url',
        'over_18',
        'edited'
    )
    post_lists = []

    for idx, post in enumerate(posts, start=1):
        post_dict = vars(post)
        post = {key: post_dict[key] for key in POST_FIELDS}
        post['id'] = idx  # Add an incremental id column
        post_lists.append(post)

    return pd.DataFrame(post_lists)


def transform_data(post_df: pd.DataFrame()):
    post_df['author'] = post_df['author'].astype(str)
    post_df['edited'] = post_df['edited'].astype(str)
    post_df['num_comments'] = post_df['num_comments'].astype(str)
    post_df['score'] = post_df['score'].astype(str)
    post_df['title'] = post_df['title'].astype(str)
    post_df.to_csv('file1.csv')

    return post_df


if __name__ == "__main__":
    parser = configparser.ConfigParser()

    client_id = 'pdwZMbYfmFmoX-6ASPFOew'
    client_secret = 'k2rEQ8U7Qc9GaS2kLaEFfpZBwJ-DDA'
    user_agent = 'Post creation error Tripathi3041'
    username='Tripathi3041'
    password='Vishal@123'
    redirect_url = 'http://localhost:8080'
    subreddit = 'dataengineering'
    time_filter = 'year'
    limit = 10

    reddit = connect_reddit(client_id, client_secret, user_agent, username, password, redirect_url)

    post_df = extract_posts(reddit, subreddit, time_filter, limit)
    df = transform_data(post_df)


