from kafka import KafkaProducer
import json
import time
import os

# Calculate data path relative to project root
# current_dir = os.path.dirname(os.path.abspath(__file__))
# project_root = os.path.dirname(os.path.dirname(current_dir))
data_path = './data/test_data.json'

print(f"Looking for data file at: {data_path}")

# If running inside Docker, Kafka on host: use host.docker.internal
broker = os.getenv("KAFKA_BROKER", "kafka:9092")
topic = os.getenv("KAFKA_TOPIC", "amazon-reviews1")

print(f"Using broker: {broker}")
print(f"Using topic: {topic}")

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers=broker,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    request_timeout_ms=10000,
    api_version=(0, 10, 1)
)

try:
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Data file not found at {data_path}")

    with open(data_path, 'r') as f:
        for line in f:
            try:
                raw = json.loads(line)

                # Extract only the needed fields
                review = {
                    "reviewerID": raw.get("reviewerID", "unknown"),
                    "overall": raw.get("overall", 0),
                    "lemmatized_text": raw.get("lemmatized_text", ""),
                    "label": raw.get("label", 0.0)
                }

                producer.send(topic, value=review)
                print(f"✅ Sent: {review['reviewerID']} → label {review['label']}")
                time.sleep(1)

            except Exception as e:
                print(f"❌ Error sending message: {e}")

    producer.flush()
    print("✅ All messages sent successfully.")

except Exception as e:
    print(f"❌ Connection or data error: {e}")

finally:
    producer.close()