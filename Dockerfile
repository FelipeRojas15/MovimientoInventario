FROM python:3.11-slim

WORKDIR /app

COPY Consumer.py .
COPY config.py .
COPY requirements.txt .
COPY .env .

RUN pip install --no-cache-dir -r requirements.txt


CMD ["python", "-u", "Consumer.py"]
