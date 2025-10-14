FROM python:3.11-slim

WORKDIR /app

COPY Consumer.py .
COPY config.py .
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt


CMD ["python", "Consumer.py"]
