

````markdown
# 📊 Real-Time Sentiment Analysis on Amazon Reviews

[![Python](https://img.shields.io/badge/python-3.9-blue.svg)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/spark-3.1.3-orange)](https://spark.apache.org/)
[![Kafka](https://img.shields.io/badge/kafka-2.13--3.4.0-black?logo=apachekafka)](https://kafka.apache.org/)
[![MongoDB](https://img.shields.io/badge/mongodb-%2334a853.svg?style=flat&logo=mongodb&logoColor=white)](https://www.mongodb.com/)
[![Docker](https://img.shields.io/badge/docker-ready-blue?logo=docker)](https://www.docker.com/)

> A real-time big data pipeline that performs sentiment analysis on streaming Amazon product reviews using **Apache Kafka**, **Spark Structured Streaming**, and **Flask** for visualization.

---

## 🔧 Tech Stack

- **Apache Kafka** — message queue for streaming reviews
- **Apache Spark** — real-time processing and model inference
- **MongoDB** — stores prediction results
- **Flask + Socket.IO** — frontend dashboard for real-time visualization
- **Docker Compose** — container orchestration

---

## 🧠 Project Architecture

```text
┌────────────┐        ┌──────────────┐        ┌──────────────┐        ┌──────────────┐
│ test_data  ├──────▶│ Kafka Broker ├──────▶│ Spark Consumer├──────▶│   MongoDB     │
└────────────┘        └──────────────┘        └─────┬────────┘        └────┬─────────┘
                                                     │                        │
                                            ┌────────▼────────┐     ┌────────▼────────┐
                                            │  Flask + Charts │◀────┤   Flask Socket  │
                                            └─────────────────┘     └─────────────────┘
````

---

## 📁 Project Structure

```
.
├── backend/                 # Flask API + real-time dashboard
├── kafka/producer/         # Kafka producer that sends JSON data
├── kafka/consumer/         # Spark streaming consumer
├── model/                  # Trained Spark ML pipeline
├── data/                   # Amazon reviews dataset
├── docker/                 # Custom Dockerfiles for services
├── docker-compose.yml      # Compose file to orchestrate all services
```

---

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/real-time-amazon-reviews.git
cd real-time-amazon-reviews
```

### 2. Build and Start All Services

```bash
docker-compose up --build
```

Wait until Spark and Kafka are fully up.

### 3. Run the Kafka Producer (sends reviews)

```bash
docker-compose up producer
```

Or:

```bash
docker exec -it kafka-producer python kafka_producer.py
```

### 4. Launch the Spark Consumer (model prediction)

```bash
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3 \
  /app/consumer/spark_consumer.py
```

### 5. Open Dashboard

Visit [http://localhost:5000](http://localhost:5000)
You should see **real-time sentiment visualizations**.

---

## 🧪 Model Training

To retrain the sentiment model (optional):

```bash
cd training/
python train_sentiment_model.py
```

This will:

* Load `cleaned_reviews.csv`
* Balance classes
* Train a `RandomForestClassifier` on TF-IDF features
* Save the model to `/model/best_model/SentimentModel/`

---

## 📦 Environment Variables

| Variable             | Default             | Description                   |
| -------------------- | ------------------- | ----------------------------- |
| `KAFKA_BROKER`       | `kafka:9092`        | Kafka broker address          |
| `KAFKA_INPUT_TOPIC`  | `amazon-reviews1`   | Kafka topic for input reviews |
| `KAFKA_OUTPUT_TOPIC` | `sentiment-results` | Kafka topic for predictions   |
| `MONGO_HOST`         | `mongodb`           | MongoDB hostname              |
| `MONGO_PORT`         | `27017`             | MongoDB port                  |

---

## 📊 Dashboard Features

* 📈 Sentiment distribution pie chart
* 📉 Sentiment trends over time
* 📦 Top products by sentiment
* 💬 Live recent reviews with predicted sentiment

---

## 🧼 Clean & Stop Everything

```bash
docker-compose down -v --remove-orphans
```

---

## 🧠 Learning Outcomes

This project helped explore:

* Real-time processing with **Spark Structured Streaming**
* Using **Kafka** as a buffer between producer/consumer
* **Machine learning pipelines** in PySpark
* Creating **interactive dashboards** with Flask + WebSockets
* Scalable multi-service deployment with **Docker Compose**

---

## 📝 License

MIT License. See `LICENSE`.

---

## 👨‍💻 Authors

* 👤 Muhammed — [GitHub](https://github.com/your-username)

---

## 🙏 Acknowledgements

* UCI Machine Learning Repository — Amazon Reviews Dataset
* Apache Spark, Kafka, Flask, Docker communities

```
