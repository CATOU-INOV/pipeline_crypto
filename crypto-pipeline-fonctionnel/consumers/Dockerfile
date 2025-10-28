FROM python:3.13-slim

WORKDIR /app

COPY price_processor.py .

RUN pip install kafka-python psycopg2-binary requests

CMD ["python", "price_processor.py"]
