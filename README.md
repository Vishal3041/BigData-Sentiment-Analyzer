# BigData-Sentiment-Analyzer

Steps to execute
1. Create Reddit API client API and secret key
2. Then run reddit_api.py file (python reddit_api.py)
3. Then file1.csv file will be generated in your local path
4. Next run main.py file it will insert the csv file data into kafka topic (posts_topic)
5. Once topic is generated we will run spark_streaming.py file (python spark_streaming.py)
6. The above step will read data from kafka topic "posts_topic" and apply SentimentAnalyzer() to it
7. Once analysis is done it is written back to kafka topic "sentiment_analysis"
8. Next a parquet file is generated locally.
