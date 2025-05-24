@echo off
docker exec -it spark-master ^
  /opt/bitnami/spark/bin/spark-submit ^
  --master spark://spark-master:7077 ^
  --deploy-mode client ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3 ^
  /app/consumer/spark_consumer.py
pause