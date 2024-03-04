from pyspark.sql.functions import from_json, col, udf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from sklearn.naive_bayes import MultinomialNB


# Define a function to perform sentiment analysis using VADER
def analyze_sentiment(text):
    analyzer = SentimentIntensityAnalyzer()
    sentiment_score = analyzer.polarity_scores(text)
    if sentiment_score['compound'] > 0.05:
        return "Positive"
    elif sentiment_score['compound'] < -0.05:
        return "Negative"
    else:
        return "Neutral"


if __name__ == "__main__":
    # Load the pre-trained Naive Bayes model
    nb_model = MultinomialNB()

    post_schema = StructType([
            StructField("id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("num_comments", StringType(), True),
            StructField("author", StringType(), True),
            StructField("url", StringType(), True),
            StructField("over_18", StringType(), True)
    ])

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://metastore:9083") \
        .appName("SentimentAnalysis") \
        .enableHiveSupport() \
        .getOrCreate()

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "posts_topic") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), post_schema).alias("data")) \
        .select("data.*")

    print("df_without_sentiment:")
    query_without_sentiment = df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    sentiment_udf = udf(analyze_sentiment, StringType())

    # Apply sentiment analysis to the 'title' column of the DataFrame
    df_with_sentiment = df.withColumn("sentiment", sentiment_udf(col("title")))
    print("df_with_sentiment:")
    query_with_sentiment = df_with_sentiment.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    kafka_write_query = df_with_sentiment \
        .selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "sentiment_analysis") \
        .option("checkpointLocation", "/path/checkpoint/checkpoint1") \
        .start()

    query = df_with_sentiment.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("checkpointLocation", "/path/checkpoint/checkpoint2") \
        .option("path", "/path/parquet_files") \
        .start()

    query_without_sentiment.awaitTermination()
    query_with_sentiment.awaitTermination()
    kafka_write_query.awaitTermination()
    query.awaitTermination()