FROM python:3.9-slim

WORKDIR /app

COPY ev_weather/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ev_weather/ .
COPY ev_common/ ./ev_common/

ENV PYTHONPATH=/app

CMD ["python", "app.py"]
