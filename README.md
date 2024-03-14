# BigData-Sentiment-Analyzer

***Objective:***
- The goal was to develop an application that performs real-time sentiment analysis on Reddit posts, providing immediate insights into public opinion on various topics.

***Pipeline Implementation:***

- ***Data Ingestion and Environment Setup:***
  - Configured a local big data environment comprising HDFS, Kafka, Zookeeper, and Hive, supplemented by cloud resources to optimize performance and scalability.
  - Installed Python and PySpark, integrating them with Kafka to facilitate data streaming from Reddit.

***API Integration and Data Streaming:***

  - Utilized PRAW (Python Reddit API Wrapper) to stream live Reddit data based on specific keywords.
  - Implemented a Python script to filter and preprocess this data, ensuring cleanliness by removing URLs and non-essential content.

***Real-time Data Analysis with Apache Spark:***

Applied a pre-trained sentiment analysis model using PySpark to assess the sentiment of each Reddit post in the stream, categorizing them into positive, negative, and neutral sentiments.
Data Storage and Visualization:

Streamed the sentiment-analyzed data back into Kafka and utilized Spark Streaming to persist the processed data into a Hive table stored in Parquet format for efficiency and performance.
Leveraged Grafana to create a dashboard that visualizes the sentiment analysis results in real-time, providing a user-friendly interface to monitor public opinion trends.
Challenges and Learnings:

Encountered and overcame technical hurdles in setting up a seamless integration between the various components of the big data stack.
Discovered best practices for streamlining data flow, such as optimizing Kafka topic configurations for robustness against potential data spikes.
Gained insights into the nuances of real-time data processing and the criticality of preprocessing for sentiment analysis accuracy.
Outcomes and Further Developments:

The Real-time Sentiment Analyzer effectively parsed through Reddit's live feed, offering stakeholders a pulse on current sentiment trends about specific topics.
The project provided valuable hands-on experience with a modern big data stack and underscored the importance of real-time analytics in understanding public sentiment.
Potential enhancements include geographical sentiment analysis, advanced NLP for finer sentiment granularity, and a comparative analysis dashboard for historical sentiment trends.

Steps to execute
1. Create Reddit API client API and secret key
2. Then run reddit_api.py file (python reddit_api.py)
3. Then file1.csv file will be generated in your local path
4. Next run main.py file it will insert the csv file data into kafka topic (posts_topic)
5. Once topic is generated we will run spark_streaming.py file (python spark_streaming.py)
6. The above step will read data from kafka topic "posts_topic" and apply SentimentAnalyzer() to it
7. Once analysis is done it is written back to kafka topic "sentiment_analysis"
8. Next a parquet file is generated locally.
