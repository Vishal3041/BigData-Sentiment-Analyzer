import pandas as pd
import simplejson as json
from confluent_kafka import SerializingProducer


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


# Define a function to serialize DataFrame rows to JSON
def serialize_df_row(row):
    string_data = {key: str(value) for key, value in row.items()}
    return string_data


if __name__ == "__main__":
    # Kafka Topics
    posts_topic = 'posts_topic'

    producer = SerializingProducer({'bootstrap.servers': 'localhost:9092', })
    df = pd.read_csv('file1.csv')

    # Drop the unnamed column if it exists
    if 'Unnamed: 0' in df.columns:
        df.drop(columns=['Unnamed: 0'], inplace=True)

    # Set 'id' column as index
    df.set_index('id', inplace=True)

    # Iterate over DataFrame rows and send each row to Kafka
    for index, row in df.iterrows():
        row['id'] = index
        data = serialize_df_row(row)
        print(data)
        producer.produce(
            posts_topic,
            key=data['id'],
            value=json.dumps(data).encode('utf-8'),
            on_delivery=delivery_report
        )

        producer.flush()