FROM bitnami/spark:3.3

USER root
WORKDIR /app

COPY spark_consumer.py ./

RUN pip install pymongo numpy

CMD ["spark-submit", "--master", "spark://spark-master:7077", "spark_consumer.py"]