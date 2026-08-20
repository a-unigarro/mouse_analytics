FROM python:3.10.20

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY consumers/ ./consumers/
COPY shared/ ./shared/

CMD ["python","-u", "-m",  "consumers.analytics_consumer"]
