FROM python:3.11-slim

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir flask flask-socketio eventlet pymongo

CMD ["python", "run.py"]
