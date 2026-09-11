FROM python:3.10.20

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY frontend/ ./frontend/
COPY backend/ ./backend/
COPY shared/ ./shared/
COPY database/ ./database/
